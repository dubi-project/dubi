'use strict';
let csrf='';
const el=id=>document.getElementById(id);
async function request(path,options={}){
  const response=await fetch(path,{credentials:'same-origin',cache:'no-store',...options});
  const value=await response.json();
  if(!response.ok)throw Object.assign(new Error(value.message||`HTTP ${response.status}`),{status:response.status});
  return value;
}
function message(text){el('message').textContent=text;}
function signedOut(text){csrf='';el('state').textContent='';el('session').hidden=true;el('login').hidden=false;message(text);}
function showError(error){if(error.status===401)signedOut('登录已失效，请重新登录。');else message(error.message);}
async function refresh(){const value=await request('/api/v1/admin/gateway');el('state').textContent=JSON.stringify(value,null,2);}
async function restore(){
  try{
    const me=await request('/api/v1/me');
    if(me.role!=='admin')throw new Error('需要管理员身份');
    csrf=me.csrf_token;el('login').hidden=true;el('session').hidden=false;
    el('identity').textContent=`已登录 · 会话到期 ${new Date(me.expires_unix*1000).toLocaleString()}`;
    await refresh();
  }catch(error){if(error.status===401)signedOut('请输入管理员 Token 登录。');else showError(error);}
}
el('login').addEventListener('submit',async event=>{
  event.preventDefault();const token=el('token').value.trim();el('token').value='';
  try{await request('/api/v1/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token})});message('');await restore();}catch(error){showError(error);}
});
el('refresh').addEventListener('click',()=>refresh().then(()=>message('已刷新')).catch(showError));
el('logout').addEventListener('click',async()=>{
  try{await request('/api/v1/auth/logout',{method:'POST',headers:{'X-Dubi-CSRF':csrf}});signedOut('已退出');}catch(error){showError(error);}
});
restore();
