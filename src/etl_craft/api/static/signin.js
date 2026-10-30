let running=false;
async function approve(node) {
  if(running)return;running=true;
  const end=Date.now()+Number(node.dataset.expiresIn)*1000;
  let interval=Math.max(1,Number(node.dataset.interval));
  while(node.isConnected && Date.now()+interval*1000<end){
    await new Promise(resolve=>setTimeout(resolve,interval*1000));
    const response=await fetch("/api/v1/login/sso/poll",{method:"POST",headers:{"Content-Type":"application/json","HX-Request":"true","X-CSRF-Token":document.querySelector('meta[name="csrf-token"]').content},body:JSON.stringify({device_code:node.dataset.deviceCode,nonce:node.dataset.nonce,_return_to:node.dataset.returnTo})});
    const redirect=response.headers.get("HX-Redirect");
    if(redirect){document.dispatchEvent(new CustomEvent("etl-craft:session",{detail:"signed-in"}));location.assign(redirect);return;}
    if(!response.ok){node.querySelector('[role="status"]').textContent="SSO sign-in refused or expired. Reload and start again.";running=false;return;}
    interval=Math.max(1,interval+(await response.json()).interval);
  }
  node.querySelector('[role="status"]').textContent="SSO approval expired. Reload and start again.";running=false;
}
document.body.addEventListener("htmx:afterSwap",()=>{const node=document.querySelector("#sso-flow");if(node)approve(node);});
