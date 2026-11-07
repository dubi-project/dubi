#!/usr/bin/env node
// Build from the explicit sibling checkout; never fetch a moving Git branch.
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = path.resolve(process.argv[2] ?? path.join(root, '../webui'));
const target = path.join(root, 'internal/gateway/httpapi/console');
const hash = data => createHash('sha256').update(data).digest('hex');
function files(directory, prefix = '') {
  return fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name, 'en')).flatMap(entry => {
    if (entry.isSymbolicLink()) throw new Error(`Symbolic links are not permitted in console inputs: ${entry.name}`);
    const relative = prefix + entry.name;
    if (entry.isDirectory()) return files(path.join(directory, entry.name), relative + '/');
    if (!entry.isFile()) throw new Error(`Unsupported console input: ${relative}`);
    return [relative];
  });
}
const packageInfo = JSON.parse(fs.readFileSync(path.join(source, 'package.json'), 'utf8'));
if (packageInfo.name !== '@dubi-project/webui') throw new Error('Expected the dubi-project/webui checkout');
execFileSync('npm', ['ci', '--ignore-scripts'], { cwd: source, stdio: 'inherit' });
execFileSync('npm', ['run', 'build'], { cwd: source, stdio: 'inherit' });
const dist = path.join(source, 'dist');
const outputFiles = files(dist);
if (!outputFiles.includes('index.html') || !outputFiles.some(name => /^assets\/.+\.js$/.test(name))) throw new Error('Console entry or JavaScript bundle is missing');
for (const name of outputFiles) {
  if (name !== 'index.html' && !name.startsWith('assets/')) throw new Error(`Unexpected public build output: ${name}`);
  if (name.endsWith('.map')) throw new Error('Production source maps must not be embedded');
}
const manifest = {
  format: 1,
  source_revision: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: source, encoding: 'utf8' }).trim(),
  source_dirty: execFileSync('git', ['status', '--porcelain'], { cwd: source, encoding: 'utf8' }).trim() !== '',
  source_sha256: '',
  files: Object.fromEntries(outputFiles.map(name => [name, hash(fs.readFileSync(path.join(dist, name)))])),
};
const sourceFiles = ['package.json', 'package-lock.json', 'tsconfig.json', 'vite.config.ts', 'index.html', ...files(path.join(source, 'src')).map(name => 'src/' + name)].sort();
manifest.source_sha256 = hash(Buffer.concat(sourceFiles.flatMap(name => [Buffer.from(name + '\0'), fs.readFileSync(path.join(source, name)), Buffer.from('\0')])));
const html = fs.readFileSync(path.join(dist, 'index.html'), 'utf8');
for (const match of html.matchAll(/(?:src|href)="([^"]+)"/g)) {
  if (!match[1].startsWith('/assets/') || !manifest.files[match[1].slice(1)]) throw new Error(`Unbundled entry dependency: ${match[1]}`);
}
// Validate the full new output before replacing the generated, tracked snapshot.
const stage = fs.mkdtempSync(path.join(path.dirname(target), '.console-build-'));
try {
  for (const name of outputFiles) {
    const destination = path.join(stage, name);
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    fs.copyFileSync(path.join(dist, name), destination);
  }
  fs.writeFileSync(path.join(stage, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
  fs.rmSync(target, { recursive: true, force: true });
  fs.renameSync(stage, target);
} finally { fs.rmSync(stage, { recursive: true, force: true }); }
console.log(`Embedded console prepared: ${outputFiles.length} files, source ${manifest.source_sha256}`);
