"""Shared, bounded SSH operations for the developer VPS acceptance tools."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import inspect
import json
from pathlib import Path
import re
import subprocess
import threading

from vps_soak_diagnostics import peer_host, ssh_json


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def executable_sha256(path):
    with Path(path).open("rb") as stream:
        header = stream.read(20)
    if header[:6] != b"\x7fELF\x02\x01" or header[18:20] != b"\x3e\x00":
        raise ValueError("provide a Linux amd64 ELF executable")
    return sha256(path)


def fingerprint(status):
    root = status["root"]
    topology = {"cluster_id": root["cluster_id"], "voters": sorted(root["voters"]),
                "learners": sorted(root.get("learners", [])),
                "peers": sorted(root["peers"], key=lambda n: n["node_id"])}
    return hashlib.sha256(json.dumps(topology, sort_keys=True).encode()).hexdigest()


def require_healthy(status):
    cluster = status["cluster"]
    if not cluster.get("root_quorum_confirmed") or cluster.get("state") != "available":
        raise RuntimeError(f"cluster has not converged: {cluster.get('reasons')}")
    observations = cluster.get("repair_observations", [])
    if len(observations) != len(status["root"]["peers"]):
        raise RuntimeError("repair observation list is incomplete")
    for node in observations:
        if (not node.get("reachable") or not node.get("worker_enabled") or not node.get("last_check_unix") or
                node.get("repair_debt") or node.get("geo_debt") or node.get("write_quarantines") or node.get("error")):
            raise RuntimeError(f"Node {node.get('name') or node['node_id']} is not healthy")


def server_options(expected):
    args=expected["server_args"]
    if not args or args[0]!="server":raise ValueError("only a standard server unit is supported")
    options={};i=1
    while i<len(args):
        name,separator,value=args[i].partition("=")
        if name not in ("--store","--listen","--api-listen","--advertise","--region","--name"):
            raise ValueError("unsupported server option; inspect the unit before Join")
        if not separator:
            i+=1
            if i>=len(args):raise ValueError("missing server option value")
            value=args[i]
        options[name]=value;i+=1
    # Persisted registration can differ from the startup arguments, e.g. after
    # Node Rename. Reuse the confirmed topology, never stale names/Regions.
    address=expected["address"]
    options["--advertise"]=address
    options["--region"]=expected["region"]
    options["--name"]=expected["name"]
    options.setdefault("--listen",("[::]:" if address.startswith("[") else "0.0.0.0:")+address.rsplit(":",1)[1])
    options.pop("--store",None)
    return [part for name,value in options.items() for part in (name,value)]

def volume_uses_mount(volume, mount):
    from pathlib import Path
    path, root = Path(volume["path"]), Path(mount)
    return volume["state"] != "removed" and (path == root or root in path.parents)


REMOTE = inspect.getsource(server_options) + "\n" + inspect.getsource(volume_uses_mount) + r'''
import base64, hashlib, json, os, pathlib, shlex, shutil, subprocess, sys, tempfile, time
request = json.loads(sys.argv[1])
def command(args, timeout=90):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return {"code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
def check(args, timeout=90):
    r=command(args, timeout)
    if r["code"]: raise RuntimeError(r["stderr"] or r["stdout"])
    return r["stdout"]
def digest(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for block in iter(lambda:f.read(1024*1024),b""): h.update(block)
    return h.hexdigest()
def managed_path(value, parent="/var/tmp", prefix="dubi-"):
    p=pathlib.Path(value)
    if not p.is_absolute() or ".." in p.parts or p.resolve()!=p or not p.is_relative_to(parent) or not p.relative_to(parent).parts[0].startswith(prefix):
        raise ValueError("invalid managed test path")
    return p
def private_directory(p):
    p.mkdir(mode=0o700,parents=True,exist_ok=True)
    if p.stat().st_uid!=0 or p.stat().st_mode & 0o022:raise RuntimeError("test directory is not private/root-owned")
def write_override(path,args):
    text="# Managed by DUBI VPS acceptance tools\n[Service]\nExecStart=\nExecStart="+shlex.join(args)+"\n"
    with tempfile.NamedTemporaryFile(mode="w",prefix=".dubi-",dir=path.parent,delete=False) as f:
        f.write(text);f.flush();os.fsync(f.fileno());temporary=pathlib.Path(f.name)
    os.replace(temporary,path)
    fd=os.open(str(path.parent),os.O_DIRECTORY);os.fsync(fd);os.close(fd)
def identity(store):
    d=json.loads((pathlib.Path(store)/"DUBI_NODE_IDENTITY").read_text())
    return {k:d[k] for k in ("node_id","raft_id","cluster_id")}
def workloads():
    found=[]
    for p in pathlib.Path("/proc").glob("[0-9]*/cmdline"):
        try: args=p.read_bytes().decode(errors="replace").split("\0")
        except OSError: continue
        if any(pathlib.Path(a).name in ("vps_cross_region_soak.sh","vps_cross_region_load.py","vps_root_failover_workload.py","vps_range_soak.py") or
               (pathlib.Path(a).name=="acceptance.py" and "dubi" in a) for a in args):
            found.append({"pid":int(p.parent.name),"command":args[:3]})
    return found
def inventory():
    pid=int(check(["systemctl","show","dubi","--property=MainPID","--value"],10).strip())
    if pid<=0: raise RuntimeError("dubi.service is not running")
    proc=pathlib.Path("/proc")/str(pid); args=(proc/"cmdline").read_bytes().decode().strip("\0").split("\0")
    path=str((proc/"exe").readlink())
    if path.endswith(" (deleted)"): raise RuntimeError("running executable differs from installed file")
    store="/var/lib/dubi"
    for i,a in enumerate(args):
        if a=="--store": store=args[i+1]
        elif a.startswith("--store="): store=a.split("=",1)[1]
    status=json.loads(check([path,"status","--store",store]))
    local=identity(store)
    if status["observed_node_id"]!=local["node_id"]: raise RuntimeError("status/store identity mismatch")
    return {"pid":pid,"binary":path,"binary_sha256":digest(proc/"exe"),"store":store,
            "identity":local,"store_id":status["local_storage"]["store_id"],"status":status,
            "server_args":args[1:],"active_workloads":workloads()}
op=request["operation"]
if op=="inspect":
    result=inventory()
elif op=="mkdir":
    p=managed_path(request["path"])
    private_directory(p)
    result={"path":str(p)}
else:
    expected=request["expected"]
    if identity(expected["store"])!=expected["identity"]: raise RuntimeError("Node identity changed since planning")
    if op not in ("cli","probe") and workloads(): raise RuntimeError("an acceptance workload is running; refuse mutation")
    binary=expected["binary"]
    if op=="cli":
        args=request["args"]
        allowed=(("status",),("version",),("tenant","create"),("record","put"),("record","get"),
                 ("node","remove"),("volume","add"),("volume","remove"),("volume","drain"))
        if not any(tuple(args[:len(prefix)])==prefix for prefix in allowed): raise ValueError("unsupported CLI operation")
        if tuple(args[:2]) not in (("record","get"),) and args[0] not in ("status","version") and workloads():
            raise RuntimeError("an acceptance workload is running; refuse mutation")
        result=command([binary,*args])
    elif op=="probe":
        helper=managed_path(request["helper"])
        if digest(helper)!=request["helper_sha256"]:raise RuntimeError("holding probe hash mismatch")
        p=subprocess.run([str(helper),"--store",expected["store"]],input=json.dumps(request["input"]),capture_output=True,text=True,timeout=90)
        result={"code":p.returncode,"stdout":p.stdout,"stderr":p.stderr}
    elif op=="stage-check":
        staged=managed_path(request["staged"])
        if digest(staged)!=request["sha256"]:raise RuntimeError("staged executable hash mismatch")
        staged.chmod(0o755);result={"sha256":request["sha256"]}
    elif op=="install":
        staged=managed_path(request["staged"])
        if digest(staged)!=request["sha256"]: raise RuntimeError("candidate hash mismatch")
        current=inventory()
        if current["store_id"]!=expected["store_id"]: raise RuntimeError("Store identity changed")
        before=digest(binary)
        if before not in (expected["binary_sha256"],request["sha256"]): raise RuntimeError("unplanned executable version")
        backup=pathlib.Path(binary+".before-"+before)
        if backup.exists() and digest(backup)!=before: raise RuntimeError("backup path contains another binary")
        if not backup.exists(): shutil.copy2(binary,backup)
        temporary=pathlib.Path(binary+".acceptance-new")
        shutil.copyfile(staged,temporary); temporary.chmod(0o755)
        with temporary.open("rb") as f: os.fsync(f.fileno())
        os.replace(temporary,binary)
        directory=os.open(str(pathlib.Path(binary).parent),os.O_DIRECTORY); os.fsync(directory); os.close(directory)
        check(["systemctl","restart","dubi"],30)
        result={"backup":str(backup),"sha256":digest(binary)}
    elif op=="service":
        if request["action"] not in ("stop","start","restart"): raise ValueError("invalid service action")
        check(["systemctl",request["action"],"dubi"],30);result={"action":request["action"]}
    elif op=="offline-volume":
        if int(check(["systemctl","show","dubi","--property=MainPID","--value"],10).strip())!=0: raise RuntimeError("stop the Node before moving a test Volume")
        p=pathlib.Path(request["path"])
        registered=json.loads((pathlib.Path(expected["store"])/"DUBI_VOLUMES").read_text())["volumes"]
        if not any(v["volume_id"]==request["volume_id"] and v["path"]==str(p) for v in registered):raise RuntimeError("Volume path no longer matches its registry")
        marker=json.loads((p/"DUBI_VOLUME").read_text())
        if marker["volume_id"]!=request["volume_id"] or marker["node_id"]!=expected["identity"]["node_id"] or marker["store_id"]!=expected["store_id"]: raise RuntimeError("Volume marker differs from plan")
        if p.is_mount(): raise RuntimeError("directory-failure simulation cannot rename a mountpoint")
        dest=p.with_name(p.name+".acceptance-offline-"+request["run_id"])
        if dest.exists(): raise RuntimeError("offline evidence already exists")
        p.rename(dest);result={"offline_path":str(dest)}
    elif op=="ticket":
        ticket=command([binary,"join","token","--store",expected["store"]])
        if ticket["code"]:raise RuntimeError("credential issuance failed; consult the issuer journal")
        result={"ticket_b64":base64.b64encode(ticket["stdout"].encode()).decode()}
    elif op=="limited-volume":
        work=managed_path(request["work"]);private_directory(work)
        image=work/"limited.img";mount=work/"limited-volume"
        if image.exists() or mount.exists():raise RuntimeError("limited test filesystem already exists")
        fs=os.statvfs(expected["store"])
        if fs.f_bavail*fs.f_frsize<8*(1<<30):raise RuntimeError("insufficient host headroom for a bounded capacity test")
        check(["truncate","-s",str(3*(1<<30)),str(image)],10)
        check(["mkfs.ext4","-F","-m","0","-q",str(image)],30)
        mount.mkdir();check(["mount","-o","loop,nosuid,nodev,noexec",str(image),str(mount)],30)
        if not mount.is_mount() or mount.stat().st_dev==pathlib.Path(expected["store"]).stat().st_dev:raise RuntimeError("capacity fixture is not a separate mounted filesystem")
        # ext4's root contains lost+found. Register an empty child directory.
        data=mount/"data";private_directory(data)
        result={"path":str(data),"image":str(image),"device":mount.stat().st_dev}
    elif op in ("reserve-space","release-space","detach-limited"):
        work=managed_path(request["work"]);mount=work/"limited-volume";image=work/"limited.img";reserve=mount/".dubi-acceptance-reserve"
        if not mount.is_mount() or mount.stat().st_dev!=request["device"]:raise RuntimeError("capacity mount identity changed")
        fs=os.statvfs(mount)
        if not 2*(1<<30)<=fs.f_blocks*fs.f_frsize<=4*(1<<30):raise RuntimeError("test filesystem size exceeds the bounded fixture")
        if op=="reserve-space":
            fd=os.open(reserve,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);os.close(fd)
            amount=max(0,fs.f_bavail*fs.f_frsize-900*(1<<20))
            if amount:check(["fallocate","-l",str(amount),str(reserve)],30)
            os.sync()
            if os.statvfs(mount).f_bavail*fs.f_frsize>1<<30:raise RuntimeError("capacity floor was not reached")
            result={"reserved_bytes":amount,"available_bytes":os.statvfs(mount).f_bavail*fs.f_frsize}
        elif op=="release-space":
            if reserve.is_symlink():raise RuntimeError("reserve path is a symlink")
            if reserve.exists():reserve.unlink()
            os.sync();result={"released":True}
        else:
            manifest=json.loads((pathlib.Path(expected["store"])/"DUBI_VOLUMES").read_text())
            if any(volume_uses_mount(v,mount) for v in manifest["volumes"]):raise RuntimeError("bounded Volume must be removed from the registry before unmount")
            check(["umount",str(mount)],30);image.unlink();result={"detached":True}
    elif op=="join-service":
        if int(check(["systemctl","show","dubi","--property=MainPID","--value"],10).strip())!=0: raise RuntimeError("old Node must be stopped before Join")
        new=managed_path(request["new_store"],"/var/lib","dubi-acceptance-")
        if new.exists(): raise RuntimeError("Join requires a fresh managed test Store")
        work=managed_path(request["work"]); private_directory(work)
        ticket=work/"join.ticket"
        fd=os.open(ticket,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,"wb") as f:f.write(base64.b64decode(request["ticket_b64"]));f.flush();os.fsync(f.fileno())
        fresh=server_options(expected)
        args=[binary,"join","--store",str(new),"--ticket-file",str(ticket),*fresh]
        if any("%" in a or "$" in a or "\n" in a for a in args):raise ValueError("unsupported systemd argument expansion")
        drop=pathlib.Path("/etc/systemd/system/dubi.service.d/90-dubi-acceptance.conf")
        drop.parent.mkdir(parents=True,exist_ok=True)
        header="# Managed by DUBI VPS acceptance tools\n"
        if drop.exists():
            if not drop.read_text().startswith(header):raise RuntimeError("a foreign acceptance override exists; inspect it first")
            shutil.copy2(drop,work/"previous-override.conf")
        write_override(drop,args)
        check(["systemctl","daemon-reload"],15);check(["systemctl","start","dubi"],30)
        result={"new_store":str(new),"override":str(drop)}
    elif op=="finish-join":
        new=managed_path(request["new_store"],"/var/lib","dubi-acceptance-")
        current=inventory()
        if current["store"]!=str(new) or current["identity"]["node_id"]!=request["new_node_id"]:raise RuntimeError("new Join identity differs from the completed test")
        fresh=server_options(expected)
        args=[binary,"server","--store",str(new),*fresh]
        if any("%" in a or "$" in a or "\n" in a for a in args):raise ValueError("unsupported systemd argument expansion")
        drop=pathlib.Path("/etc/systemd/system/dubi.service.d/90-dubi-acceptance.conf")
        header="# Managed by DUBI VPS acceptance tools\n"
        if not drop.read_text().startswith(header):raise RuntimeError("acceptance override ownership changed")
        write_override(drop,args)
        check(["systemctl","daemon-reload"],15);check(["systemctl","restart","dubi"],30)
        ticket=managed_path(request["work"])/"join.ticket"
        if ticket.exists():ticket.unlink()
        result={"override":str(drop)}
    else: raise ValueError("unknown remote operation")
print(json.dumps(result))
'''


class Fleet:
    def __init__(self, key, evidence):
        self.key = Path(key).expanduser()
        self.evidence = Path(evidence)
        self.lock = threading.Lock()

    def request(self, host, operation, **args):
        request = {"operation": operation, **args}
        try:
            # Keep one-time credentials off both local and remote argv/ps.
            # repr creates a quoted Python string literal; its contents are
            # decoded only as JSON data, never executed as Python or shell.
            program = REMOTE.replace("request = json.loads(sys.argv[1])", "request = json.loads(" + repr(json.dumps(request)) + ")", 1)
            result = ssh_json(host, self.key, program)
            safe = {k: v for k, v in result.items() if k != "ticket_b64"}
            event = {"utc": now(), "host": host, "operation": operation, "result": safe}
            return result
        except Exception as err:
            event = {"utc": now(), "host": host, "operation": operation, "error": str(err)}
            raise
        finally:
            with self.lock:
                self.evidence.mkdir(parents=True, exist_ok=True)
                with (self.evidence / "events.jsonl").open("a") as stream:
                    stream.write(json.dumps(event, ensure_ascii=False) + "\n")

    def inspect(self, host):
        return self.request(host, "inspect")

    def discover(self, host):
        first = self.inspect(host)
        peers = first["status"]["root"]["peers"]

        def one(peer):
            node = self.inspect(peer_host(peer["address"]))
            if node["identity"]["node_id"] != peer["node_id"] or node["identity"]["raft_id"] != peer["raft_id"]:
                raise RuntimeError("Root address resolved to a different Node")
            node.update(name=peer.get("name", ""), region=peer["region_id"], address=peer["address"], host=peer_host(peer["address"]))
            return node

        with ThreadPoolExecutor(max_workers=3) as pool:
            nodes = list(pool.map(one, peers))
        if any(fingerprint(n["status"]) != fingerprint(first["status"]) for n in nodes):
            raise RuntimeError("membership changed while inventory was collected")
        for key in ("node_id", "raft_id"):
            if len({n["identity"][key] for n in nodes}) != len(nodes):
                raise RuntimeError("duplicate Node identities in inventory")
        if len({n["store_id"] for n in nodes}) != len(nodes):
            raise RuntimeError("duplicate Store identities in inventory")
        return {"utc": now(), "entry_host": host, "topology_sha256": fingerprint(first["status"]), "nodes": nodes}

    def copy(self, source, host, destination):
        # Remote paths are generated by this tool; host validation also applies
        # to scp destinations (OpenSSH options must never come from a peer).
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:\-]*", host):
            raise ValueError("invalid SSH host")
        if not destination.startswith("/var/tmp/dubi-") or any(c in destination for c in " '\n\r`$"):
            raise ValueError("invalid staging path")
        source = str(Path(source).resolve(strict=True))
        subprocess.run(["scp", "-C", "-i", str(self.key), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                        "-o", "ConnectTimeout=8", str(source), f"root@{'['+host+']' if ':' in host else host}:{destination}"], check=True, timeout=90)

    def cli(self, node, args, *, allow_failure=False):
        result = self.request(node["host"], "cli", expected=node, args=args)
        if result["code"] and not allow_failure:
            raise RuntimeError(result["stderr"] or result["stdout"])
        return result


def no_workload(inventory):
    busy = [(n["name"], n["active_workloads"]) for n in inventory["nodes"] if n["active_workloads"]]
    if busy:
        raise RuntimeError(f"active acceptance workload; changes refused: {busy}")


def validate_plan(plan, inventory):
    if not re.fullmatch(r"[a-f0-9]{32}", plan.get("run_id", "")):
        raise ValueError("invalid plan identifier")
    if inventory["topology_sha256"] != plan["inventory"]["topology_sha256"]:
        raise RuntimeError("membership changed since planning; create a new plan")
    expected = {n["identity"]["node_id"]: n for n in plan["inventory"]["nodes"]}
    for node in inventory["nodes"]:
        old = expected[node["identity"]["node_id"]]
        if node["store"] != old["store"] or node["store_id"] != old["store_id"] or node["binary"] != old["binary"]:
            raise RuntimeError("Node Store or executable path changed since planning")
    no_workload(inventory)
