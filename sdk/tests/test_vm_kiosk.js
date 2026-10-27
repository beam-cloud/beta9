const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source=fs.readFileSync(process.argv[2],'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
const flush=()=>new Promise(resolve=>setImmediate(resolve));
async function check(path) {
  let onRetry,onRestart,application='starting',responseStatus=200,health=true;
  const requests=[],navigations=[],timers=new Map();
  const elements={
    '#status':{},'#retry':{addEventListener:(_,fn)=>{onRetry=fn;}},
    '#restart':{addEventListener:(_,fn)=>{onRestart=fn;}},
  };
  const origin='https://application.example';
  vm.runInNewContext(source,{
    URL,AbortSignal,Date,location:{href:origin+path,replace:url=>navigations.push(url)},
    document:{querySelector:selector=>elements[selector]},
    setTimeout:fn=>{timers.set(1,fn);return 1;},clearTimeout:id=>timers.delete(id),
    fetch:async (url,options={})=>{
      const pathname=new URL(url).pathname;requests.push({path:pathname,...options});
      if (pathname.endsWith('/restart')) return {ok:true};
      if (pathname.endsWith('/health')) return {ok:health};
      return {ok:responseStatus===200,status:responseStatus,json:async()=>({state:application,application,error:'Application closed'})};
    },
  });
  const advance=async()=>{const fn=timers.get(1);timers.delete(1);fn();await flush();};
  await flush();assert.equal(navigations.length,0);
  application='closed';await advance();assert.equal(elements['#restart'].hidden,false);
  await onRestart();await flush();
  assert.equal(requests.find(r=>r.method==='POST').headers['X-Beam-Kiosk-Action'],'restart');
  application='ready';health=false;await advance();assert.equal(navigations.length,0,'wait for stream health');
  health=true;await advance();
  assert.equal(navigations.at(-1),origin+path+'stream/');
  assert.equal(requests.at(-1).path,path+'stream/api/health');
  responseStatus=403;onRetry();await flush();
  assert.equal(elements['#retry'].hidden,false);
  assert.match(elements['#status'].textContent,/access is unavailable/);
}
(async()=>{for(const path of ['/','/vm/app/8080/'])await check(path);})().catch(error=>{console.error(error);process.exitCode=1;});
