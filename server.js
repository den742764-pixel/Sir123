'use strict';
/* Сириус — сервер. Без зависимостей: только Node.js 18+.  Запуск: node server.js */
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto'),net=require('net');
/* Read a local .env for development only; real hosting should use its secret manager. Never overwrite variables already supplied by the host. */
try{const envFile=path.join(__dirname,'.env');if(fs.existsSync(envFile)){for(const line of fs.readFileSync(envFile,'utf8').split(/\r?\n/)){const m=line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);if(!m||Object.prototype.hasOwnProperty.call(process.env,m[1]))continue;let v=m[2];if((v.startsWith('\"')&&v.endsWith('\"'))||(v.startsWith("'")&&v.endsWith("'")))v=v.slice(1,-1);process.env[m[1]]=v}}}catch(e){console.error('Не удалось прочитать локальную конфигурацию окружения:',e.message);process.exit(1)}
const PORT=+process.env.PORT||3000,DIR=process.env.DATA_DIR||path.join(__dirname,'data'),MEDIA=path.join(DIR,'media'),FILE=path.join(DIR,'db.json');
const REQUIRE_HTTPS=process.env.REQUIRE_HTTPS==='1'||(process.env.NODE_ENV==='production'&&process.env.REQUIRE_HTTPS!=='0');
if(process.env.NODE_ENV==='production'&&process.env.REQUIRE_HTTPS==='0')console.error('WARNING: REQUIRE_HTTPS=0 отключает принудительный HTTPS в production.');
/* Optional shared rate limiter: configure REDIS_URL for multi-instance deployments. */
let redisClient=null,redisReady=false;
if(process.env.REDIS_URL){
  try{
    const {createClient}=require('redis');
    redisClient=createClient({url:process.env.REDIS_URL});
    redisClient.on('error',e=>{redisReady=false;console.error('Redis limiter unavailable:',e.message)});
    redisClient.connect().then(()=>{redisReady=true;console.log('Redis distributed limiter connected')}).catch(e=>{redisReady=false;console.error('Redis limiter connection failed:',e.message)});
  }catch(e){console.error('REDIS_URL is set but package redis is unavailable:',e.message)}
}
const sharedLimits={
  '/api/auth/register':[10,900], '/api/rec/start':[60,900], '/api/rec/do':[12,900],
  '/api/dev/redeem':[8,900], '/api/sig/send':[120,60], '/api/w':[600,60], default:[600,60]
};
async function distributedLimit(pathname,ip){
  if(!process.env.REDIS_URL)return true;
  if(!redisClient||!redisReady||!redisClient.isReady)return false;
  const [max,seconds]=sharedLimits[pathname]||sharedLimits.default;
  const digest=crypto.createHash('sha256').update(String(ip)).digest('hex').slice(0,32);
  const key='sirius:rl:v1:'+pathname.replace(/[^a-z0-9/_-]/gi,'_')+':'+digest;
  const n=Number(await redisClient.eval("local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]); end; return n",{keys:[key],arguments:[String(seconds)]}));
  return n<=max;
}

fs.mkdirSync(DIR,{recursive:true,mode:0o700});fs.mkdirSync(MEDIA,{recursive:true,mode:0o700});try{fs.chmodSync(DIR,0o700);fs.chmodSync(MEDIA,0o700)}catch(e){}
const COLS=['users','friends','blocks','posts','groups','stories','calls','msgs','gifts'];
const MAPS=['mq','mediaOwner','tokens','tokenMeta','rec','bal','orders','admins','gcount','premium','support',...COLS];
/* Все «словари» без прототипа: ключи вроде __proto__ / constructor не могут ничего испортить */
const RESERVED=new Set(['__proto__','constructor','prototype','hasOwnProperty','toString','valueOf','bot']);
const DB={audit:[],ordn:0,secret:'',cfg:{}};MAPS.forEach(k=>DB[k]=Object.create(null));
/* Скользящий срок сессии: активные пользователи не разлогиниваются по календарю; год полного бездействия отзывает токен. */
const SESSION_IDLE_MAX_AGE=365*24*60*60*1000;
const SESSION_TOUCH_INTERVAL=5*60*1000;
/* Загрузка. Повреждённый файл НЕ затираем пустой базой — откладываем в сторону и сообщаем. */
try{if(fs.existsSync(FILE)){try{fs.chmodSync(FILE,0o600);if(fs.existsSync(FILE+'.bak'))fs.chmodSync(FILE+'.bak',0o600)}catch(e){}let d=null;
  try{d=JSON.parse(fs.readFileSync(FILE,'utf8'))}catch(e){const bad=FILE+'.corrupt-'+Date.now();fs.renameSync(FILE,bad);console.error('ВНИМАНИЕ: db.json повреждён и сохранён как '+bad+'. Попробуйте восстановить из db.json.bak');}
  if(d&&typeof d==='object')Object.keys(DB).forEach(k=>{if(d[k]!==undefined&&d[k]!==null)DB[k]=MAPS.includes(k)?Object.assign(Object.create(null),d[k]):d[k]})}}
catch(e){console.error('Не удалось прочитать базу данных:',e.message);process.exit(1)}
for(const[id,c]of Object.entries(DB.calls))if(Date.now()-(c.t||0)>36e5)delete DB.calls[id];
/* Миграция старых сессий: сохраняем реальное время последней активности, но переводим срок на год бездействия. */
for(const [u,t] of Object.entries(DB.tokens)){if(u==='bot'||!DB.users[u])continue;const now=Date.now();let m=DB.tokenMeta[u];if(!m||typeof m!=='object')m=DB.tokenMeta[u]={created:now,last:now};if(!Number.isFinite(m.created))m.created=now;if(!Number.isFinite(m.last)||m.last>now)m.last=now;m.exp=m.last+SESSION_IDLE_MAX_AGE;}
/* Запись: одна за раз (без гонок на .tmp), раз в 10 минут — копия db.json.bak */
let timer=0,writing=0,dirty=0,lastBak=0;
const writeDB=()=>{if(writing){dirty=1;return}writing=1;const tmp=FILE+'.tmp';
  fs.writeFile(tmp,JSON.stringify(DB),{mode:0o600},e=>{if(e){writing=0;return console.error('Ошибка записи базы:',e.message)}
    const done=()=>fs.rename(tmp,FILE,e2=>{if(e2)console.error('Ошибка замены базы:',e2.message);else try{fs.chmodSync(FILE,0o600)}catch(e){}writing=0;if(dirty){dirty=0;save()}});
    if(Date.now()-lastBak>6e5&&fs.existsSync(FILE)){lastBak=Date.now();fs.copyFile(FILE,FILE+'.bak',e3=>{if(!e3)try{fs.chmodSync(FILE+'.bak',0o600)}catch(e){}done()})}else done()})};
const save=()=>{if(timer)return;timer=setTimeout(()=>{timer=0;writeDB()},500)};
const validPublicKeyJwk=s=>{try{const k=typeof s==='string'?JSON.parse(s):s;if(!k||k.kty!=='EC'||k.crv!=='P-256'||typeof k.x!=='string'||typeof k.y!=='string'||Object.prototype.hasOwnProperty.call(k,'d')||!/^[A-Za-z0-9_-]{43}$/.test(k.x)||!/^[A-Za-z0-9_-]{43}$/.test(k.y))return false;crypto.createPublicKey({key:{kty:'EC',crv:'P-256',x:k.x,y:k.y},format:'jwk'});return true}catch(e){return false}};
/* Validate image bytes, not just the attacker-controlled data: URL MIME label. */
const validImageDataUrl=value=>{try{
  if(typeof value!=='string'||value.length>250000)return false;
  const m=/^data:image\/(png|jpeg|webp|gif);base64,([A-Za-z0-9+/]+={0,2})$/.exec(value);if(!m)return false;
  const b64=m[2],buf=Buffer.from(b64,'base64');if(!buf.length||buf.toString('base64')!==b64)return false;
  const type=m[1];
  if(type==='png')return buf.length>=8&&buf.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]));
  if(type==='jpeg')return buf.length>=3&&buf[0]===255&&buf[1]===216&&buf[2]===255;
  if(type==='gif')return buf.length>=6&&(buf.subarray(0,6).toString('ascii')==='GIF87a'||buf.subarray(0,6).toString('ascii')==='GIF89a');
  if(type==='webp')return buf.length>=12&&buf.subarray(0,4).toString('ascii')==='RIFF'&&buf.subarray(8,12).toString('ascii')==='WEBP';
  return false;
}catch(e){return false}};
const hash=t=>crypto.createHash('sha256').update(String(t)).digest('hex');
if(!DB.secret)DB.secret=crypto.randomBytes(32).toString('hex');
DB.tokens.bot=hash(crypto.randomBytes(16));/* id 'bot' зарезервирован: занять его нельзя */
DB.users.bot={name:'Сириус Бот',un:'sirius_bot',bio:'Служебный бот: зачисления ★ и уведомления',av:'',hidden:true,seen:0};
save();
const eq=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const sd=(a,b)=>{a=a||[];b=b||[];return[...a.filter(x=>!b.includes(x)),...b.filter(x=>!a.includes(x))]};
const isAdm=(g,u)=>!!g&&(g.by===u||(g.adm||[]).includes(u)),isMem=(g,u)=>!!g&&(g.mem||[]).includes(u);
const deny=m=>Object.assign(new Error(m||'Запрещено'),{code:403});
const PUB=['name','un','bio','av','cv','st','bd','hbd','seen','invisible','hidden','fonly','nopost','fph','nocall','pk'];
const viewUser=(v,u,id)=>{if(id===u)return Object.assign({},v,{prem:(DB.premium[id]||0)>Date.now()});const o={};PUB.forEach(k=>{if(v[k]!==undefined&&!(k==='seen'&&v.invisible))o[k]=v[k]});o.prem=(DB.premium[id]||0)>Date.now();/* fph means friends only: never expose a phone number to strangers */if(v.fph&&v.ph&&isFr(id,u))o.ph=v.ph;return o};
function canSee(c,v,u){
  if(c==='msgs'){const g=DB.groups[v.c];if(g)return isMem(g,u)||!!(g.ch&&g.pub);return String(v.c).split('_').includes(u)}
  if(c==='blocks')return v.by===u||v.to===u;
  if(c==='calls')return v.from===u||v.to===u;
  if(c==='friends'||c==='gifts')return v.from===u||v.to===u;/* граф друзей и подарки — только участникам */
  if(c==='groups')return v.by===u||isMem(v,u)||!!(v.ch&&v.pub);
  if(c==='stories')return v.by===u||isFr(v.by,u);
  return true}
function state(u){const s={};for(const c of COLS){s[c]={};for(const[id,v]of Object.entries(DB[c])){if(canSee(c,v,u))s[c][id]=c==='users'?viewUser(v,u,id):v}}return s}
const clients=new Set(),STREAM_TICKETS=new Map();
// Keep live-stream accounting O(1): scanning all sockets for every new connection
// makes connection attempts increasingly expensive under load.
const streamCountByUser=new Map(),streamCountByIP=new Map();
const bumpCount=(map,key,delta)=>{const n=(map.get(key)||0)+delta;if(n>0)map.set(key,n);else map.delete(key)};
const removeClient=cl=>{if(!clients.delete(cl))return;if(cl.ka)clearInterval(cl.ka);bumpCount(streamCountByUser,cl.u,-1);bumpCount(streamCountByIP,cl.ip,-1)};
// Reserve media IDs and per-user quota while asynchronous disk writes are pending.
const MEDIA_PENDING_IDS=new Set(),MEDIA_PENDING_BYTES=Object.create(null);
const SSE_MAX_BUFFER=4*1024*1024;
const send=(cl,o)=>{try{const r=cl.res;if(!r||r.destroyed||r.writableEnded){removeClient(cl);return false}const frame='data: '+JSON.stringify(o)+'\n\n',bytes=Buffer.byteLength(frame);if(bytes>SSE_MAX_BUFFER||r.writableLength+bytes>SSE_MAX_BUFFER){removeClient(cl);try{r.end()}catch(_){}return false}r.write(frame);return true}catch(e){removeClient(cl);try{cl.res.end()}catch(_){}return false}};
function broadcast(c,id,v){
  if(c==='groups'){clients.forEach(cl=>send(cl,{t:'init',s:state(cl.u)}));return}
  clients.forEach(cl=>{if(v===undefined)send(cl,{t:'del',c,id});else if(canSee(c,v,cl.u))send(cl,{t:'set',c,id,v:c==='users'?viewUser(v,cl.u,id):v});else send(cl,{t:'del',c,id})})}
function canDel(c,id,old,u){
  switch(c){
    case'users':return id===u;
    case'friends':return id.split('_').includes(u);
    case'blocks':return old.by===u;
    case'posts':case'stories':return old.by===u;
    case'groups':return old.by===u;
    case'calls':return old.from===u||old.to===u;
    case'msgs':{const g=DB.groups[old.c];return old.from===u||(g?isAdm(g,u):String(old.c).split('_').includes(u))}}
  return false}
function canWrite(c,id,old,nv,u){
  switch(c){
    case'users':{if(id!==u)return false;if(String(nv.name||'').length>60||String(nv.bio||'').length>1000||String(nv.st||'').length>200)return false;if(nv.pk!==undefined&&!validPublicKeyJwk(nv.pk))return false;if(nv.un!==undefined){const un=String(nv.un);if(!/^[a-z0-9_]{3,20}$/.test(un))return false;if(Object.entries(DB.users).some(([i,x])=>i!==id&&x.un===un))return false}return true}
    case'friends':{const[f,t]=id.split('_');if(!f||!t||f===t||nv.from!==f||nv.to!==t)return false;if(!old)return u===f&&nv.st==='p';return(u===t&&nv.st==='ok')||(u===f&&nv.st==='p'&&old.st==='p')}
    case'blocks':{const[b,t]=id.split('_');return u===b&&nv.by===b&&nv.to===t}
    case'posts':{if(!old)return nv.by===u;if(old.by===u)return nv.by===old.by;
      return sd(old.lk,nv.lk).every(x=>x===u)&&eq((old.cm||[]).filter(x=>x.by!==u),(nv.cm||[]).filter(x=>x.by!==u))&&(nv.cm||[]).filter(x=>x.by===u).length<=200&&Object.keys({...old,...nv}).every(k=>k==='lk'||k==='cm'||eq(old[k],nv[k]))}
    case'groups':{
      if(!validGroupShape(nv))return false;
      if(nv.un&&Object.entries(DB.groups).some(([i,x])=>i!==id&&x.un===nv.un))return false;
      if(!old)return nv.by===u&&nv.mem.includes(u);
      if(nv.by!==old.by)return false;
      if(isAdm(old,u))return true;
      const ch=sd(old.mem,nv.mem);if(!ch.every(x=>x===u))return false;
      if((nv.mem||[]).includes(u)&&!(old.mem||[]).includes(u)&&!old.ch)return false;
      const oa=old.adm||[old.by],na=nv.adm||oa;if(!sd(oa,na).every(x=>x===u)||(na.includes(u)&&!oa.includes(u)))return false;
      return Object.keys({...old,...nv}).every(k=>k==='mem'||k==='adm'||eq(old[k],nv[k]))}
    case'msgs':{const g=DB.groups[old?old.c:nv.c];
      if(nv.kind==='gift'||(old&&old.kind==='gift')){if(!old||old.kind!=='gift'||nv.kind!=='gift'||nv.body!==old.body||nv.gn!==old.gn||!eq(nv.gd,old.gd))return false}
      if(!old){
        if(nv.from!==u||typeof nv.c!=='string')return false;
        if(g)return isMem(g,u)&&(!g.ch||isAdm(g,u));
        /* Direct conversations must have exactly two real participants and a canonical ID.
           This prevents forged multi-user IDs and applies server-side block / friends-only policy. */
        const ps=nv.c.split('_');
        if(ps.length!==2||ps[0]===ps[1]||ps.some(x=>!DB.users[x])||ps.includes('bot')||!ps.includes(u)||nv.c!==ps.slice().sort().join('_'))return false;
        const peer=ps[0]===u?ps[1]:ps[0];
        if(isBl(u,peer)||isBl(peer,u))return false;
        if(DB.users[peer].fonly&&!isFr(u,peer))return false;
        // Direct-message payloads must use the encrypted envelope format. Do not
        // trust the client-supplied e=2 flag on a plaintext `text` message.
        return nv.kind==='e2e'&&(nv.e===1||nv.e===2);
      }
      if(nv.c!==old.c||nv.from!==old.from)return false;
      /* Участник, которого удалили из группы, не может продолжать редактировать старые сообщения. В каналах изменять сообщения могут только действующие администраторы. */
      if(old.from===u){
        if(g)return isMem(g,u)&&(!g.ch||isAdm(g,u));
        const ps=String(old.c).split('_');if(ps.length!==2||!ps.includes(u))return false;
        const peer=ps[0]===u?ps[1]:ps[0];if(isBl(u,peer)||isBl(peer,u))return false;
        return nv.e===1||nv.e===2||nv.body===old.body;
      }
      if(!canSee('msgs',old,u))return false;
      const rxk=new Set([...Object.keys(old.rx||{}),...Object.keys(nv.rx||{})]);
      for(const k of rxk)if(!sd((old.rx||{})[k],(nv.rx||{})[k]).every(x=>x===u))return false;
      return Object.keys({...old,...nv}).every(k=>k==='rx'||k==='rd'||eq(old[k],nv[k]))}
    case'stories':{if(!old)return nv.by===u;if(old.by===u)return nv.by===u;
      const ks=new Set([...Object.keys(old.rx||{}),...Object.keys(nv.rx||{})]);for(const k of ks)if(k!==u&&!eq((old.rx||{})[k],(nv.rx||{})[k]))return false;
      return Object.keys({...old,...nv}).every(k=>k==='rx'||eq(old[k],nv[k]))}
    case'calls':{if(!old)return nv.from===u&&!!nv.to;return(old.from===u||old.to===u)&&nv.from===old.from&&nv.to===old.to}
  }
  return false}
const dropStreams=u=>{for(const cl of [...clients])if(cl.u===u){removeClient(cl);try{cl.res.end()}catch(e){}}for(const[k,v]of STREAM_TICKETS)if(v.u===u)STREAM_TICKETS.delete(k)};
function purgeUser(u){
  // Remove messages from groups owned by the deleted account as well as direct
  // messages and records explicitly associated with that account.
  const ownedGroups=new Set(Object.entries(DB.groups).filter(([,g])=>g.by===u).map(([id])=>id));
  for(const c of ['friends','blocks','posts','stories','calls','gifts','msgs'])for(const[id,v]of Object.entries(DB[c])){
    const mine=(c==='posts'||c==='stories')?v.by===u:c==='blocks'?(v.by===u||v.to===u):c==='msgs'?(v.from===u||String(v.c).split('_').includes(u)||ownedGroups.has(String(v.c))):(v.from===u||v.to===u);
    if(mine)delete DB[c][id]}
  for(const[id,g]of Object.entries(DB.groups)){if(g.by===u)delete DB.groups[id];else{g.mem=(g.mem||[]).filter(x=>x!==u);if(g.adm)g.adm=g.adm.filter(x=>x!==u)}}
  // Account deletion also removes account-linked order and audit records rather
  // than leaving a long-lived trail keyed to the deleted account ID.
  for(const[id,o]of Object.entries(DB.orders))if(o.u===u)delete DB.orders[id];
  DB.audit=DB.audit.filter(a=>a&&a.u!==u);
  for(const p of Object.values(DB.posts)){if(Array.isArray(p.lk))p.lk=p.lk.filter(x=>x!==u);if(Array.isArray(p.cm))p.cm=p.cm.filter(x=>x.by!==u)}
  for(const m of Object.values(DB.msgs))if(m.rx)for(const k of Object.keys(m.rx))if(Array.isArray(m.rx[k]))m.rx[k]=m.rx[k].filter(x=>x!==u);
  /* При удалении аккаунта физически удаляем и его медиа, а не только запись владельца. */
  for(const[id,owner]of Object.entries(DB.mediaOwner)){if(owner===u){delete DB.mediaOwner[id];try{fs.unlinkSync(path.join(MEDIA,id))}catch(e){if(e.code!=='ENOENT')console.error('Не удалось удалить медиа аккаунта:',e.message)}}}
  delete DB.users[u];delete DB.rec[u];delete DB.admins[u];delete DB.bal[u];delete DB.premium[u];for(const[id,x]of Object.entries(DB.support))if(x.u===u)delete DB.support[id];delete DB.mq[u];delete DB.tokenMeta[u];
  DB.tokens[u]=hash(crypto.randomBytes(32));/* «надгробие»: этот ID никто больше не сможет занять */
  DB.audit.push({t:Date.now(),n:0,why:'account deleted'});
  save();clients.forEach(cl=>cl.u!==u&&send(cl,{t:'init',s:state(cl.u)}));
  setTimeout(()=>dropStreams(u),500)}/* потоки закрываем после ответа клиенту */
const USER_PATCH_KEYS=new Set(['name','un','bio','av','cv','st','bd','hbd','seen','invisible','fonly','nopost','fph','nocall','pk']);
function validUserPatch(p){
  if(!p||typeof p!=='object'||Array.isArray(p)||Object.keys(p).some(k=>!USER_PATCH_KEYS.has(k)))return false;
  for(const k of ['name','un','bio','av','cv','st','bd','hbd'])if(p[k]!==undefined&&typeof p[k]!=='string')return false;
  if(p.name!==undefined&&(p.name.trim().length<1||p.name.length>60))return false;
  if(p.bio!==undefined&&p.bio.length>1000)return false;
  if(p.st!==undefined&&p.st.length>200)return false;
  for(const k of ['av','cv'])if(p[k]!==undefined){if(p[k].length>120000)return false;if(p[k].startsWith('data:')&&!validImageDataUrl(p[k]))return false;}
  for(const k of ['bd','hbd'])if(p[k]!==undefined&&p[k].length>40)return false;
  for(const k of ['invisible','fonly','nopost','fph','nocall'])if(p[k]!==undefined&&typeof p[k]!=='boolean')return false;
  if(p.seen!==undefined&&(!Number.isFinite(p.seen)||p.seen<0||p.seen>Date.now()+60000))return false;
  if(p.pk!==undefined&&!validPublicKeyJwk(p.pk))return false;
  if(p.un!==undefined&&!/^[a-z0-9_]{3,20}$/.test(p.un))return false;
  return true;
}
function validGroupShape(g){
  if(!g||typeof g!=='object'||!Array.isArray(g.mem)||g.mem.length>500)return false;
  if(g.mem.some(x=>typeof x!=='string'||!DB.users[x])||new Set(g.mem).size!==g.mem.length||!g.mem.includes(g.by))return false;
  const admins=Array.isArray(g.adm)?g.adm:[g.by];
  return admins.length<=g.mem.length&&new Set(admins).size===admins.length&&admins.every(x=>g.mem.includes(x))&&admins.includes(g.by);
}
/* Messages are untrusted input even when encrypted: validate their shape before
   storing/broadcasting them to avoid oversized metadata and malformed state. */
const canonicalB64=s=>typeof s==='string'&&s.length>0&&s.length%4===0&&/^[A-Za-z0-9+/]+={0,2}$/.test(s)&&Buffer.from(s,'base64').toString('base64')===s;
function validE2EBody(version,body){
  try{
    if(!canonicalB64(body))return false;
    const outer=Buffer.from(body,'base64');
    if(version===1)return outer.length>=28&&outer.length<=400000;
    if(version!==2||outer.length>500000)return false;
    const env=JSON.parse(outer.toString('utf8'));
    if(!env||env.v!==2||!canonicalB64(env.s)||!canonicalB64(env.i)||!canonicalB64(env.c))return false;
    const salt=Buffer.from(env.s,'base64'),iv=Buffer.from(env.i,'base64'),ct=Buffer.from(env.c,'base64');
    return salt.length===16&&iv.length===12&&ct.length>=16&&ct.length<=400000;
  }catch(e){return false}
}
function validMessageShape(m){
  if(!m||typeof m!=='object'||Array.isArray(m))return false;
  const allowed=new Set(['c','from','t','kind','body','gn','gd','rx','rd','e']);
  if(Object.keys(m).some(k=>!allowed.has(k)))return false;
  if(typeof m.c!=='string'||m.c.length<1||m.c.length>120||/[\u0000-\u001f\u007f]/.test(m.c))return false;
  if(typeof m.from!=='string'||m.from.length>40||!DB.users[m.from])return false;
  if(!Number.isFinite(m.t)||m.t<0||m.t>Date.now()+5*60*1000)return false;
  if(typeof m.kind!=='string'||m.kind.length<1||m.kind.length>32)return false;
  if(m.body!==undefined&&(typeof m.body!=='string'||m.body.length>500000))return false;
  if(m.gn!==undefined&&(typeof m.gn!=='string'||m.gn.length>80))return false;
  if(m.rx!==undefined){
    if(!m.rx||typeof m.rx!=='object'||Array.isArray(m.rx)||Object.keys(m.rx).length>500)return false;
    for(const [k,a] of Object.entries(m.rx))if(k.length>40||!Array.isArray(a)||a.length>500||a.some(x=>typeof x!=='string'||x.length>40))return false;
  }
  if(m.e!==undefined&&m.e!==1&&m.e!==2)return false;
  if(m.kind==='e2e'&&!validE2EBody(m.e,m.body))return false;
  if(m.rd!==undefined&&typeof m.rd!=='boolean'&&(!Number.isFinite(m.rd)||m.rd<0||m.rd>1e9))return false;
  return true;
}
function apply(u,op,c,id,v){
  const old=DB[c][id];
  // Group IDs must never collide with canonical direct-conversation IDs (UID_UID).
  if(c==='groups'&&!old&&/^\d{8}_\d{8}$/.test(id))throw err('ID группы не может совпадать с ID личного диалога',400);
  if(op==='delete'){if(!old)return;if(!canDel(c,id,old,u))throw deny();if(c==='users'){purgeUser(u);return}delete DB[c][id];save();broadcast(c,id,undefined);return}
  v=v&&typeof v==='object'&&!Array.isArray(v)?v:{};
  // Treat the client as untrusted: profile updates may only touch public, user-editable fields.
  // In particular, clients cannot write server-managed phone, hidden, role, or developer flags.
  if(c==='users'&&(id!==u||!validUserPatch(v)))throw deny('Недопустимые поля профиля');
  const nv=op==='update'?Object.assign({},old||{},v):v;
  if(c==='groups'&&!validGroupShape(nv))throw deny('Некорректный состав группы или список администраторов');
  if(c==='msgs'&&!validMessageShape(nv))throw err('Некорректная структура сообщения',400);
  if(!canWrite(c,id,old,nv,u))throw deny();
  DB[c][id]=nv;save();broadcast(c,id,nv)}
function auth(u,t,ip){
  if(typeof u!=='string'||!/^[A-Za-z0-9_\-]{3,40}$/.test(u)||RESERVED.has(u)||typeof t!=='string'||t.length<32||t.length>200)return false;
  if(locked('af:'+ip,30,9e5))throw err('Слишком много попыток. Подождите 15 минут.',429);
  const stored=DB.tokens[u];
  // ВАЖНО: аутентификация никогда не создаёт учётные записи. Иначе можно было
  // занять неизвестный ID, отправив первый запрос с произвольным токеном.
  const meta=DB.tokenMeta[u],now=Date.now();
  // Срок скользящий: год после последней активности. Раз в 5 минут продлеваем
  // обе отметки, поэтому активный пользователь не теряет вход каждые 30 дней.
  const valid=typeof stored==='string'&&ceq(stored,hash(t))&&!!DB.users[u]&&meta&&Number.isFinite(meta.exp)&&meta.exp>now&&Number.isFinite(meta.last)&&meta.last<=now&&now-meta.last<SESSION_IDLE_MAX_AGE;
  if(valid){if(now-meta.last>=SESSION_TOUCH_INTERVAL){meta.last=now;meta.exp=now+SESSION_IDLE_MAX_AGE;save()}return true}
  if(meta&&(meta.exp<=now||!Number.isFinite(meta.last)||now-meta.last>=SESSION_IDLE_MAX_AGE)){delete DB.tokenMeta[u];DB.tokens[u]=hash(crypto.randomBytes(32));save();dropStreams(u)}
  fail('af:'+ip);return false;
}

/* ================= Восстановление профиля, магазин ★, подарки ================= */
const err=(m,c)=>Object.assign(new Error(m),{code:c||400});
const HOPS=Math.max(0,Math.floor(+process.env.TRUST_PROXY)||0);/* число доверенных прокси в цепочке */
/* Forwarded headers are trusted only when the immediate TCP peer is explicitly allowlisted.
   TRUST_PROXY alone is not an identity check: without this allowlist a direct client could
   forge X-Forwarded-Proto and bypass HTTPS enforcement or poison IP-based rate limits. */
const TRUSTED_PROXIES=new net.BlockList();
const normalizeIP=ip=>String(ip||'').replace(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/i,'$1');
try{
  for(const entry of String(process.env.TRUST_PROXY_IPS||'').split(',').map(x=>x.trim()).filter(Boolean)){
    const m=/^(.+)\/(\d{1,3})$/.exec(entry),ip=normalizeIP(m?m[1]:entry),family=net.isIP(ip);
    if(!family)throw new Error('invalid IP in TRUST_PROXY_IPS: '+entry);
    if(m){const prefix=Number(m[2]),max=family===4?32:128;if(prefix>max)throw new Error('invalid subnet in TRUST_PROXY_IPS: '+entry);TRUSTED_PROXIES.addSubnet(ip,prefix,family===4?'ipv4':'ipv6')}
    else TRUSTED_PROXIES.addAddress(ip,family===4?'ipv4':'ipv6');
  }
}catch(e){console.error('FATAL: invalid TRUST_PROXY_IPS:',e.message);process.exit(1)}
const trustedProxy=req=>{const ip=normalizeIP(req.socket.remoteAddress||''),family=net.isIP(ip);return !!family&&TRUSTED_PROXIES.check(ip,family===4?'ipv4':'ipv6')};
const ipOf=req=>{const peer=String(req.socket.remoteAddress||'');if(!HOPS||!trustedProxy(req))return normalizeIP(peer);const x=String(req.headers['x-forwarded-for']||'').split(',').map(a=>normalizeIP(a.trim())).filter(a=>net.isIP(a));if(x.length>=HOPS)return x[x.length-HOPS];return normalizeIP(peer)};
const jp=t=>JSON.parse(t,(k,v)=>k==='__proto__'?undefined:v);/* разбор JSON без __proto__ */
const ceq=(a,b)=>{a=Buffer.from(String(a));b=Buffer.from(String(b));return a.length===b.length&&crypto.timingSafeEqual(a,b)};
/* Password-proof checks use async scrypt so hostile recovery traffic cannot synchronously freeze the HTTP event loop. */
const shAsync=(p,salt)=>new Promise((resolve,reject)=>crypto.scrypt(String(p),String(salt),32,(e,key)=>e?reject(e):resolve(key.toString('hex'))));
const nq=q=>String(q||'').trim().toLowerCase().replace(/^@/,'');
const FL=Object.create(null);/* Неудачные попытки: защита от подбора паролей и кодов. Хранилище ограничено, чтобы уникальные IP/имена не могли бесконечно раздувать память. */
const FL_MAX_KEYS=20000,FL_MAX_EVENTS=100,FL_WINDOW=15*60*1000;
const locked=(k,max,ms)=>{const n=Date.now(),a=(FL[k]||[]).filter(t=>n-t<ms);if(a.length)FL[k]=a;else delete FL[k];return a.length>=max};
const fail=k=>{const n=Date.now();let a=FL[k];if(!a){if(Object.keys(FL).length>=FL_MAX_KEYS){let count=FL_MAX_KEYS;for(const key of Object.keys(FL)){const kept=FL[key].filter(t=>n-t<FL_WINDOW);if(kept.length)FL[key]=kept;else{delete FL[key];count--}if(count<FL_MAX_KEYS)break;}if(count>=FL_MAX_KEYS){/* В условиях исчерпания лимитера не выделяем бесконечную память под новые ключи. */return}}a=FL[k]=[]}else a=FL[k]=a.filter(t=>n-t<FL_WINDOW);a.push(n);if(a.length>FL_MAX_EVENTS)a.splice(0,a.length-FL_MAX_EVENTS)};
// General POST traffic uses a compact bounded counter map (not the failed-attempt
// event list above, whose per-key history is intentionally capped at 100 entries).
const POST_LIMITS=new Map(),POST_LIMIT_MAX_KEYS=20000,POST_LIMIT_WINDOW=60000,POST_LIMIT_MAX=600;
function postRateLimited(ip){
  const now=Date.now();let b=POST_LIMITS.get(ip);
  if(!b){if(POST_LIMITS.size>=POST_LIMIT_MAX_KEYS){for(const[k,v]of POST_LIMITS)if(now-v.start>=POST_LIMIT_WINDOW)POST_LIMITS.delete(k);if(POST_LIMITS.size>=POST_LIMIT_MAX_KEYS)return true}b={start:now,count:0};POST_LIMITS.set(ip,b)}
  if(now-b.start>=POST_LIMIT_WINDOW){b.start=now;b.count=0}b.count++;return b.count>POST_LIMIT_MAX;
}
setInterval(()=>{const n=Date.now();for(const k of Object.keys(FL)){FL[k]=FL[k].filter(t=>n-t<FL_WINDOW);if(!FL[k].length)delete FL[k]}for(const[k,v]of POST_LIMITS)if(n-v.start>=POST_LIMIT_WINDOW)POST_LIMITS.delete(k)},6e5).unref();
const pushBal=u=>{const v=DB.bal[u]||0;clients.forEach(cl=>{if(cl.u===u)send(cl,{t:'bal',v})})};
const credit=(u,n,why)=>{DB.bal[u]=(DB.bal[u]||0)+n;DB.audit.push({t:Date.now(),u,n,why});if(DB.audit.length>1000)DB.audit.splice(0,DB.audit.length-1000);save();pushBal(u)};
const botMsg=(u,tx)=>{const id='mb'+Date.now().toString(36)+crypto.randomBytes(8).toString('hex'),v={c:['bot',u].sort().join('_'),from:'bot',t:Date.now(),kind:'text',body:tx,rx:{},rd:0};DB.msgs[id]=v;save();broadcast('msgs',id,v)};
const adm=u=>{if(!isAdmin(u))throw err('Нет прав владельца',403)};
const userByUn=un=>Object.entries(DB.users).find(([i,x])=>i!=='bot'&&x.un===nq(un));

/* Платёжные реквизиты: переменные окружения — значения по умолчанию; владелец может безопасно менять их из панели. */
const OWNER={card:(process.env.OWNER_CARD||'').replace(/\D/g,''),holder:String(process.env.OWNER_NAME||'').slice(0,80),bank:String(process.env.OWNER_BANK||'').slice(0,80),url:String(process.env.PAY_URL||'').slice(0,300)};
const RATE=+process.env.STAR_RUB||1,MINS=+process.env.MIN_STARS||10,MAXS=+process.env.MAX_STARS||100000;
const paymentConfig=()=>{const p=DB.cfg&&DB.cfg.payment&&typeof DB.cfg.payment==='object'?DB.cfg.payment:{};return{card:typeof p.card==='string'?p.card:OWNER.card,holder:typeof p.holder==='string'?p.holder:OWNER.holder,bank:typeof p.bank==='string'?p.bank:OWNER.bank,url:typeof p.url==='string'?p.url:OWNER.url,rate:Number.isFinite(p.rate)&&p.rate>0?p.rate:RATE,min:Number.isInteger(p.min)&&p.min>=1?p.min:MINS,max:Number.isInteger(p.max)&&p.max>=MINS?p.max:MAXS}};
/* Код разработчика встроен ТОЛЬКО в серверную часть по запросу владельца. Никогда не переносить в index.html.
   Для продакшена безопаснее заменить раскрытый код новым секретом через DEV_CODE в настройках хостинга. */
const DEV=String(process.env.DEV_CODE||'08#12)22&21@12?22<Den>/code*');
const weakDevCode=/replace[-_ ]?with|change[-_ ]?me|example|placeholder|test[-_ ]?only|your[-_ ]?secret|password/i.test(DEV);
if(DEV.length<28||(process.env.NODE_ENV==='production'&&weakDevCode)){console.error('FATAL: DEV_CODE must be a unique random secret of at least 32 characters; known placeholder values are forbidden in production.');process.exit(1)}
/* Права владельца привязаны к текущему DEV_CODE: сменили код на хостинге — все выданные ранее права владельца сгорают */
const devTag=()=>DEV.length>=8?hash('admin:'+DEV):'';
const isAdmin=u=>!!DB.admins[u]&&DB.admins[u]===devTag();
for(const k of Object.keys(DB.admins))if(DB.admins[k]===true&&devTag())DB.admins[k]=devTag();/* миграция со старой версии */
/* Каталог подарков. r: 0 обычный · 1 редкий · 2 эпический · 3 легендарный · 4 мифический.
   p — цена в ★, s — тираж (сколько экземпляров вообще может существовать; без s — безлимит).
   Чем реже подарок — тем меньше тираж и выше цена. Правьте список под себя. */
const GIFTS=[
 {id:'rose',e:'🌹',n:'Роза',r:0,p:10},
 {id:'choc',e:'🍫',n:'Шоколад',r:0,p:15},
 {id:'cake',e:'🍰',n:'Тортик',r:0,p:25},
 {id:'bear',e:'🧸',n:'Плюшевый мишка',r:1,p:75,s:10000},
 {id:'bouq',e:'💐',n:'Букет',r:1,p:100,s:10000},
 {id:'ball',e:'🎈',n:'Воздушный шар',r:1,p:60,s:10000},
 {id:'bfly',e:'🦋',n:'Хрустальная бабочка',r:2,p:250,s:1000},
 {id:'ring',e:'💍',n:'Кольцо',r:2,p:300,s:1000},
 {id:'cup',e:'🏆',n:'Кубок чемпиона',r:2,p:400,s:1000},
 {id:'drag',e:'🐉',n:'Дракон',r:3,p:1000,s:100},
 {id:'gem',e:'💎',n:'Бриллиант',r:3,p:1200,s:100},
 {id:'crown',e:'👑',n:'Корона',r:3,p:1500,s:100},
 {id:'planet',e:'🪐',n:'Личная планета',r:4,p:5000,s:10},
 {id:'sirius',e:'🌟',n:'Звезда «Сириус»',r:4,p:10000,s:5}
];
const gLeft=g=>g.s?Math.max(0,g.s-(DB.gcount[g.id]||0)):null;
const isFr=(a,b)=>(DB.friends[a+'_'+b]||DB.friends[b+'_'+a]||{}).st==='ok';
const isBl=(a,b)=>!!(DB.blocks[a+'_'+b]||DB.blocks[b+'_'+a]);
const API={
 'auth/register':{open:1,f:async(unused,b,req)=>{
   const ip=ipOf(req),tok=String(b.token||''),name=String(b.name||'').trim(),un=nq(b.un),pk=String(b.pk||'');
   const rk='register:'+ip;if(locked(rk,10,9e5))throw err('Слишком много регистраций. Попробуйте через 15 минут.',429);fail(rk);
   if(!/^[A-Za-z0-9_-]{32,128}$/.test(tok))throw err('Неверный токен сессии');
   const salt=String(b.salt||''),proof=String(b.proof||''),vault=String(b.vault||'');
   if(name.length<1||name.length>60||!/^[a-z0-9_]{3,20}$/.test(un)||pk.length<20||pk.length>4096||!validPublicKeyJwk(pk))throw err('Некорректные данные профиля или ключ шифрования');
   if(!/^[A-Za-z0-9+/=]{16,64}$/.test(salt)||!/^[0-9a-f]{64}$/.test(proof)||!/^[A-Za-z0-9+/=]{20,8192}$/.test(vault))throw err('Некорректные данные восстановления');
   if(Object.values(DB.users).some(x=>x&&x.un===un))throw err('Юзернейм уже занят',409);
   let uid='';for(let i=0;i<100;i++){const candidate=String(crypto.randomInt(10000000,100000000));if(!RESERVED.has(candidate)&&!DB.tokens[candidate]&&!DB.users[candidate]){uid=candidate;break}}
   if(!uid)throw err('Не удалось выделить ID. Повторите попытку.',503);
   // Сервер назначает ID сам, чтобы нельзя было занять заранее выбранный чужой ID.
   // Профиль, токен и зашифрованный резерв ключей создаются атомарно.
   const ss=crypto.randomBytes(16).toString('hex');
   DB.tokens[uid]=hash(tok);DB.tokenMeta[uid]={created:Date.now(),last:Date.now(),exp:Date.now()+SESSION_IDLE_MAX_AGE};DB.users[uid]={name,un,bio:'',pk,seen:Math.min(Date.now(),Math.max(0,+b.seen||Date.now())),invisible:false};
   DB.rec[uid]={salt,ss,h:await shAsync(proof,ss),vault};
   /* Единственный стартовый бонус: 50 ★ один раз при создании аккаунта. Задания не начисляют валюту. */
   DB.bal[uid]=50;DB.audit.push({t:Date.now(),u:uid,n:50,why:'welcome bonus'});
   save();broadcast('users',uid,DB.users[uid]);return{ok:1,uid,bal:50};
 }},
 'auth/revoke':{f:u=>{DB.tokens[u]=hash(crypto.randomBytes(32));delete DB.tokenMeta[u];save();dropStreams(u);return{ok:1}}},
 'account/delete':{f:async(u,b)=>{
   // Удаление аккаунта требует повторного знания облачного пароля, а не только
   // активного токена: украденной сессии одной недостаточно для уничтожения аккаунта.
   const r=DB.rec[u],proof=String(b.proof||''),key='delete:'+u;
   if(locked(key,5,9e5))throw err('Слишком много попыток подтверждения. Подождите 15 минут.',429);
   if(!r||!/^[0-9a-f]{64}$/.test(proof)||!ceq(await shAsync(proof,r.ss),r.h)){fail(key);throw err('Не удалось подтвердить облачный пароль',403)}
   purgeUser(u);return{ok:1}
 }},
 'session/streamticket':{f:(u)=>{if(locked('ticket:'+u,60,60000))throw err('Слишком много подключений',429);fail('ticket:'+u);if(STREAM_TICKETS.size>=5000){const now=Date.now();for(const[k,v]of STREAM_TICKETS)if(v.exp<=now)STREAM_TICKETS.delete(k);if(STREAM_TICKETS.size>=5000)throw err('Слишком много ожидающих подключений. Повторите позже.',503)}const ticket=crypto.randomBytes(32).toString('hex');/* Ticket is bound to the exact active session, not merely to an account ID. */STREAM_TICKETS.set(ticket,{u,sessionHash:DB.tokens[u],exp:Date.now()+45000});return{ticket}}},
 'rtc/config':{f:u=>{const iceServers=[{urls:['stun:stun.l.google.com:19302','stun:stun.cloudflare.com:3478']}];const turnSecret=process.env.TURN_SHARED_SECRET,turnUrls=(process.env.TURN_URL||'').split(',').map(x=>x.trim()).filter(Boolean);if(turnSecret&&turnSecret.length>=32&&turnUrls.length){/* coturn REST API: short-lived per-user credentials (TURN_SHARED_SECRET must match static-auth-secret on the TURN server). */const expiry=Math.floor(Date.now()/1000)+Math.max(300,Math.min(3600,+process.env.TURN_TTL_SECONDS||900));const username=expiry+':'+u;const credential=crypto.createHmac('sha1',turnSecret).update(username).digest('base64');iceServers.push({urls:turnUrls,username,credential})}return{iceServers,turnConfigured:iceServers.length>1}}},
 'sig/send':{f:(u,b)=>{
   const to=String(b.to||''),kind=String(b.kind||''),allowed=new Set(['invite','accept','offer','answer','ice','end','reject']);
   if(!DB.users[to]||to===u||!allowed.has(kind))throw err('Неверный адресат или сигнал');
   if(isBl(u,to)||isBl(to,u)||!isFr(u,to))throw err('Звонки доступны только друзьям, если никто не заблокирован',403);
   const key='sig:'+u;if(locked(key,90,60000))throw err('Слишком много сигналов звонка',429);fail(key);
   let data=b.data;
   if(data!==undefined){const raw=JSON.stringify(data);if(raw.length>50000)throw err('Слишком большой сигнал',413);if(!data||typeof data!=='object'||Array.isArray(data))throw err('Неверный сигнал')}
   let sent=0;for(const cl of clients)if(cl.u===to){send(cl,{t:'sig',from:u,kind,data:data||null,call:String(b.call||'').slice(0,80)});sent++}
   return {ok:1,online:sent>0};
 }},
 /* --- восстановление профиля по облачному паролю (сервер пароль не видит: только производное значение) --- */
 'rec/start':{open:1,f:(u,b,req)=>{const k='rs:'+ipOf(req);if(locked(k,60,9e5))throw err('Слишком много запросов',429);fail(k);
   // Не раскрываем существование аккаунта через формат UID: для неизвестного имени
   // возвращаем фиктивный UID того же формата и фиктивную соль. rec/do всё равно
   // выполняет scrypt и отвечает одной общей ошибкой. Ограничиваем ввод до поиска.
   const raw=String(b.un||'').trim().toLowerCase().replace(/^@/,'').slice(0,64);
   const un=/^[a-z0-9_]{3,20}$/.test(raw)?raw:'invalid:'+hash(raw).slice(0,20);
   const f=Object.entries(DB.users).find(([i,x])=>x.un===un&&DB.rec[i]);
   if(f)return{uid:f[0],salt:DB.rec[f[0]].salt};
   const fk=crypto.createHmac('sha256',DB.secret).update('fake:'+un).digest();
   let fakeUid=String(10000000+(fk.readUInt32BE(0)%90000000));
   while(DB.users[fakeUid])fakeUid=String(10000000+((Number(fakeUid)-10000000+1)%90000000));
   return{uid:fakeUid,salt:fk.subarray(0,16).toString('base64')}}},
 'rec/do':{open:1,f:async(u,b,req)=>{const rawUid=String(b.uid||''),uid=/^\d{8}$/.test(rawUid)?rawUid:'invalid:'+hash(rawUid.slice(0,128)).slice(0,20),rawTok=String(b.tok||''),rawPr=String(b.proof||''),tok=rawTok.length<=128?rawTok:'',pr=rawPr.length<=128?rawPr:'',k1='r:'+uid,k2='ri:'+ipOf(req);
   if(locked(k1,5,9e5)||locked(k2,12,9e5))throw err('Слишком много попыток. Подождите 15 минут.',429);
   const real=DB.rec[uid],r=real||{ss:'0'.repeat(32),h:'0'.repeat(64)},fmt=/^[0-9a-f]{64}$/.test(pr)&&/^[A-Za-z0-9_-]{32,128}$/.test(tok);
   const candidateProof=await shAsync(pr,r.ss);const okp=fmt&&ceq(candidateProof,r.h)&&!!real;/* scrypt считается и для несуществующих аккаунтов — по времени ответа их не отличить */
   if(!okp){fail(k1);fail(k2);throw err('Неверный username или облачный пароль',403)}
   DB.tokens[uid]=hash(tok);DB.tokenMeta[uid]={created:Date.now(),last:Date.now(),exp:Date.now()+SESSION_IDLE_MAX_AGE};save();dropStreams(uid);return{vault:r.vault,salt:r.salt}}},/* новый токен: старые сессии теряют доступ и реальное время */
 'rec/vault':{f:u=>{const r=DB.rec[u];if(!r)throw err('Резервная копия не найдена',404);return{salt:r.salt,vault:r.vault}}},
 'rec/set':{f:async(u,b)=>{const salt=String(b.salt||''),pr=String(b.proof||''),vault=String(b.vault||'');
   if(!/^[A-Za-z0-9+/=]{16,64}$/.test(salt)||!/^[0-9a-f]{64}$/.test(pr)||!/^[A-Za-z0-9+/=]{20,8192}$/.test(vault))throw err('Неверные данные');
   const o=DB.rec[u];if(o){const k='rc:'+u;if(locked(k,5,9e5))throw err('Слишком много попыток',429);if(!/^[0-9a-f]{64}$/.test(String(b.old||''))||!ceq(await shAsync(b.old,o.ss),o.h)){fail(k);throw err('Неверный текущий облачный пароль',403)}}
   const ss=crypto.randomBytes(16).toString('hex');DB.rec[u]={salt,ss,h:await shAsync(pr,ss),vault};
   // Смена облачного пароля отзывает украденные/старые сессии. Текущему клиенту
   // выдаём новый случайный токен, чтобы он мог продолжить работу без повторного входа.
   const nextToken=crypto.randomBytes(32).toString('hex');DB.tokens[u]=hash(nextToken);DB.tokenMeta[u]={created:Date.now(),last:Date.now(),exp:Date.now()+SESSION_IDLE_MAX_AGE};save();dropStreams(u);
   return{ok:1,token:nextToken}}},
 /* --- Поддержка и системные боты --- */
 'support/create':{f:(u,b)=>{const message=String(b.message||'').trim().slice(0,2000);if(message.length<2)throw err('Напишите сообщение длиннее двух символов');const rk='support:'+u;if(locked(rk,8,60000))throw err('Слишком много обращений. Повторите позже.',429);fail(rk);const id='sp'+Date.now().toString(36)+crypto.randomBytes(4).toString('hex');DB.support[id]={id,u,un:(DB.users[u]||{}).un||'',message,t:Date.now(),st:'open',reply:''};save();return{ok:1,id,message:'Обращение №'+id+' отправлено в поддержку. Ответ появится в системных уведомлениях.'}}},
 'premium/state':{f:u=>{const until=Number(DB.premium[u]||0);return{active:until>Date.now(),until:until>Date.now()?until:0,bal:DB.bal[u]||0,price:500,giftPrice:650,days:30,plans:[{days:30,price:500},{days:90,price:1350},{days:180,price:2400},{days:365,price:4000}],benefits:['Значок звезды Сириус у имени','Неоновая рамка профиля и анимация свечения','Эксклюзивные обложки профиля','Коллекция Premium-аватаров','Увеличенный лимит символов в описании профиля','Приоритетная очередь поддержки','Расширенные темы и цвета интерфейса','Эксклюзивная метка в чатах','Дополнительные реакции и эмодзи','Ранний доступ к новым функциям','Особый статус Premium','Поддержка развития Сириуса']}}},
 'premium/buy':{f:(u,b)=>{const rk='premium-buy:'+u;if(locked(rk,5,60000))throw err('Слишком много попыток. Повторите позже.',429);fail(rk);const plans={30:500,90:1350,180:2400,365:4000},days=Number(b.days||30);if(!Object.prototype.hasOwnProperty.call(plans,days))throw err('Недопустимый срок Premium');const price=plans[days];if((DB.bal[u]||0)<price)throw err('Недостаточно ★. Нужно '+price+' ★');DB.bal[u]-=price;const now=Date.now(),until=Math.max(now,Number(DB.premium[u]||0))+days*86400000;DB.premium[u]=until;DB.audit.push({t:now,u,n:-price,why:'premium '+days+' days'});save();pushBal(u);broadcast('users',u,DB.users[u]);botMsg(u,'⭐ Sirius Premium активирован на '+days+' дней! Ваши привилегии уже доступны.');return{ok:1,until,bal:DB.bal[u],days,price}}},
 'premium/gift':{f:(u,b)=>{const rk='premium-gift:'+u;if(locked(rk,5,60000))throw err('Слишком много попыток. Повторите позже.',429);fail(rk);const un=nq(b.un),target=Object.entries(DB.users).find(([id,v])=>id!==u&&id!=='bot'&&v.un===un);if(!target)throw err('Пользователь не найден');const to=target[0],price=650;if((DB.bal[u]||0)<price)throw err('Недостаточно ★. Нужно '+price+' ★');const now=Date.now();DB.bal[u]-=price;const until=Math.max(now,Number(DB.premium[to]||0))+30*86400000;DB.premium[to]=until;DB.audit.push({t:now,u,n:-price,why:'gift premium to '+un});save();pushBal(u);broadcast('users',to,DB.users[to]);botMsg(to,'🎁 Вам подарили ⭐ Sirius Premium на 30 дней!');botMsg(u,'💎 Вы подарили Premium пользователю @'+un+'. Спасибо!');return{ok:1,until,bal:DB.bal[u],to:un}}},
 /* --- магазин ★ --- */
 'shop/state':{f:u=>{const p=paymentConfig();return{bal:DB.bal[u]||0,admin:isAdmin(u),cfg:{rate:p.rate,min:p.min,max:p.max,ready:!!p.card,card:p.card,holder:p.holder,bank:p.bank,url:p.url},
   gifts:GIFTS.map(g=>({...g,left:gLeft(g)})),
   orders:Object.values(DB.orders).filter(o=>o.u===u).sort((a,b)=>b.t-a.t).slice(0,10).map(({id,n,amt,rub,st,t})=>({id,n,amt,rub,st,t}))}}},
 'shop/order':{f:(u,b)=>{const rk='order:'+u;if(locked(rk,10,60000))throw err('Слишком много заказов. Повторите позже.',429);fail(rk);const p=paymentConfig();if(!p.card)throw err('Приём оплаты ещё не настроен владельцем');const a=Math.floor(+b.amt);
   if(!(a>=p.min&&a<=p.max))throw err('Количество: от '+p.min+' до '+p.max+' ★');
   if(Object.values(DB.orders).filter(o=>o.u===u&&(o.st==='new'||o.st==='claimed')).length>=5)throw err('Слишком много неоплаченных заказов — отмените ненужные');
   const o={id:'o'+Date.now().toString(36)+crypto.randomBytes(3).toString('hex'),n:++DB.ordn,u,amt:a,rub:Math.round(a*p.rate*100)/100,st:'new',t:Date.now()};DB.orders[o.id]=o;save();
   return{order:{id:o.id,n:o.n,amt:o.amt,rub:o.rub,st:o.st,t:o.t}}}},
 'shop/paid':{f:(u,b)=>{const o=DB.orders[b.id];if(!o||o.u!==u||o.st!=='new')throw err('Заказ недоступен');o.st='claimed';o.note=String(b.note||'').slice(0,80);o.pt=Date.now();save();return{ok:1}}},
 'shop/cancel':{f:(u,b)=>{const o=DB.orders[b.id];if(!o||o.u!==u||o.st!=='new')throw err('Заказ недоступен');o.st='cancel';save();return{ok:1}}},
 /* --- подарок: списывает ★ у отправителя, пишет карточку в чат, добавляет в «Подарки» получателя --- */
 'gift/send':{f:(u,b)=>{const rk='gift:'+u;if(locked(rk,20,60000))throw err('Слишком много подарков за короткое время. Повторите позже.',429);fail(rk);const to=String(b.to||''),g=GIFTS.find(x=>x.id===b.gid);
   if(!g)throw err('Нет такого подарка');if(RESERVED.has(to)||!DB.users[to]||to===u)throw err('Нельзя отправить подарок этому пользователю');
   // Enforce block privacy symmetrically: either party's block must prevent
   // gifts and the system-generated chat card, not just ordinary DM writes.
   if(isBl(u,to)||isBl(to,u))throw err('Недоступно',403);if(DB.users[to].fonly&&!isFr(u,to))throw err('Пользователь принимает сообщения только от друзей',403);
   if(g.s&&(DB.gcount[g.id]||0)>=g.s)throw err('Этот подарок распродан');
   if((DB.bal[u]||0)<g.p)throw err('Недостаточно ★ — пополните баланс');
   DB.bal[u]-=g.p;const k=DB.gcount[g.id]=(DB.gcount[g.id]||0)+1,now=Date.now(),rnd=crypto.randomBytes(8).toString('hex'),
   gid='gf'+now.toString(36)+rnd,mid='m'+now.toString(36)+u.slice(-4)+rnd,gd={e:g.e,n:g.n,r:g.r,s:g.s||0},
   mv={c:[u,to].sort().join('_'),from:u,t:now,kind:'gift',body:g.id,gn:k,gd,rx:{},rd:0};
   DB.msgs[mid]=mv;DB.gifts[gid]={from:u,to,gid:g.id,...gd,k,t:now};DB.audit.push({t:now,u,n:-g.p,why:'gift '+g.id+' → '+to});save();
   broadcast('msgs',mid,mv);broadcast('gifts',gid,DB.gifts[gid]);pushBal(u);return{ok:1,bal:DB.bal[u]}}},
 /* --- код разработчика: открывает режим владельца и зачисляет ★ (код задаётся ТОЛЬКО на сервере: DEV_CODE) --- */
 'dev/redeem':{f:(u,b,req)=>{if(DEV.length<32)throw err('Код разработчика не настроен безопасно: DEV_CODE должен содержать минимум 32 символа',403);
   const k='d:'+u,k2='di:'+ipOf(req);if(locked(k,5,9e5)||locked(k2,15,9e5))throw err('Слишком много попыток. Подождите 15 минут.',429);
   const h=x=>crypto.createHash('sha256').update(String(x)).digest();if(!crypto.timingSafeEqual(h(b.code||''),h(DEV))){fail(k);fail(k2);throw err('Неверный код',403)}
   const n=Math.floor(+b.amt)||0;if(n<0||n>1e9)throw err('Неверное количество');DB.admins[u]=devTag();if(n)credit(u,n,'dev code');else save();return{bal:DB.bal[u]||0,admin:true}}},
 /* --- панель владельца --- */
 'admin/list':{f:u=>{adm(u);const p=paymentConfig();return{payment:{card:p.card,holder:p.holder,bank:p.bank,url:p.url,rate:p.rate,min:p.min,max:p.max},orders:Object.values(DB.orders).filter(o=>o.st==='claimed'||o.st==='new').sort((a,b)=>(b.st==='claimed')-(a.st==='claimed')||b.t-a.t).slice(0,50).map(o=>({id:o.id,n:o.n,amt:o.amt,rub:o.rub,st:o.st,note:o.note||'',un:(DB.users[o.u]||{}).un})),support:Object.values(DB.support).filter(x=>x.st==='open').sort((a,b)=>a.t-b.t).slice(0,30).map(x=>({id:x.id,un:x.un,message:x.message,t:x.t}))}}},
 'admin/support-reply':{f:(u,b)=>{adm(u);const x=DB.support[String(b.id||'')];if(!x||x.st!=='open')throw err('Обращение уже закрыто или не найдено');const reply=String(b.reply||'').trim().slice(0,2000);if(reply.length<1)throw err('Введите ответ');x.reply=reply;x.st='answered';x.rt=Date.now();save();botMsg(x.u,'🛟 Ответ поддержки по обращению '+x.id+':\n'+reply);return{ok:1}}},
 'admin/paycfg':{f:(u,b)=>{adm(u);const card=String(b.card||'').replace(/\D/g,'').slice(0,34),holder=String(b.holder||'').trim().slice(0,80),bank=String(b.bank||'').trim().slice(0,80),url=String(b.url||'').trim().slice(0,300);if(card&&!(card.length>=10&&card.length<=34))throw err('Укажите номер карты, счёта или телефона длиной 10–34 цифры');if(url&&!/^https:\/\//i.test(url))throw err('Ссылка оплаты должна начинаться с https://');if(!card&&(holder||bank||url))throw err('Укажите реквизит или очистите остальные поля');DB.cfg=DB.cfg&&typeof DB.cfg==='object'?DB.cfg:{};const old=paymentConfig();DB.cfg.payment={card,holder,bank,url,rate:old.rate,min:old.min,max:old.max,updated:Date.now()};save();return{ok:1,payment:{card,holder,bank,url}}}},
 'admin/confirm':{f:(u,b)=>{adm(u);const o=DB.orders[b.id];if(!o||(o.st!=='claimed'&&o.st!=='new'))throw err('Заказ недоступен');o.st='done';o.dt=Date.now();
   credit(o.u,o.amt,'order #'+o.n);botMsg(o.u,'✅ Заказ №'+o.n+' оплачен: на ваш баланс зачислено '+o.amt+' ★. Приятных подарков! 🎁');return{ok:1}}},
 'admin/reject':{f:(u,b)=>{adm(u);const o=DB.orders[b.id];if(!o||(o.st!=='claimed'&&o.st!=='new'))throw err('Заказ недоступен');o.st='rej';save();
   botMsg(o.u,'❌ Заказ №'+o.n+' отклонён: перевод не найден. Если вы оплатили — свяжитесь с владельцем приложения.');return{ok:1}}},
 'admin/grant':{f:(u,b)=>{adm(u);const f=userByUn(b.un);if(!f)throw err('Пользователь не найден');const n=Math.floor(+b.amt);if(!(n>0&&n<=1e9))throw err('Неверное количество');
   credit(f[0],n,'grant by admin');botMsg(f[0],'🎁 Вам начислено '+n+' ★');return{ok:1}}}
};
const MAX_BODY_BYTES=600000;
const body=req=>new Promise((resolve,reject)=>{let n=0,done=false;const chunks=[];const tooLarge=()=>{if(done)return;done=true;reject(Object.assign(new Error('Слишком большой запрос'),{code:413}));/* Drain the request instead of destroying the socket before the 413 response can be sent. */req.resume()};const len=Number(req.headers['content-length']);if(Number.isFinite(len)&&len>MAX_BODY_BYTES){tooLarge();return}req.on('data',chunk=>{if(done)return;n+=chunk.length;if(n>MAX_BODY_BYTES){tooLarge();return}chunks.push(chunk)});req.on('end',()=>{if(!done){done=true;resolve(Buffer.concat(chunks).toString())}});req.on('error',e=>{if(!done){done=true;reject(e)}})});
/* CSP: скрипты разрешены только по SHA-256 хэшу встроенного <script> — внедрённый чужой скрипт не выполнится */
const cspFor=html=>{const hs=[...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)].map(m=>"'sha256-"+crypto.createHash('sha256').update(m[1]).digest('base64')+"'");
  return "default-src 'self'; script-src 'self' "+hs.join(' ')+"; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; media-src 'self' data: blob:; connect-src 'self' stun: turn: turns:; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"};
const server=http.createServer(async(req,res)=>{
  // Baseline browser-security headers apply even to malformed requests and early errors.
  res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('X-Frame-Options','DENY');res.setHeader('Referrer-Policy','no-referrer');
  res.setHeader('X-Permitted-Cross-Domain-Policies','none');res.setHeader('Cross-Origin-Resource-Policy','same-origin');
  res.setHeader('Permissions-Policy','camera=(self), microphone=(self), geolocation=()');res.setHeader('Cross-Origin-Opener-Policy','same-origin');res.setHeader('Origin-Agent-Cluster','?1');res.setHeader('X-DNS-Prefetch-Control','off');res.setHeader('X-Download-Options','noopen');
  if(typeof req.url!=='string'||req.url.length>8192){res.writeHead(414,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Connection':'close'});return res.end(JSON.stringify({err:'Слишком длинный адрес запроса'}))}
  let url;try{url=new URL(req.url,'http://x')}catch(e){res.writeHead(400,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Connection':'close'});return res.end(JSON.stringify({err:'Некорректный адрес запроса'}))}
  if(url.pathname.length>2048||url.search.length>4096){res.writeHead(414,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Connection':'close'});return res.end(JSON.stringify({err:'Слишком длинный адрес запроса'}))}
  const origin= req.headers.origin; const allowedOrigin=(process.env.ALLOW_ORIGIN||'').split(',').map(x=>x.trim()).filter(Boolean); const cors={'Vary':'Origin','Access-Control-Allow-Headers':'Content-Type','Access-Control-Allow-Methods':'GET,POST,OPTIONS'}; if(origin && allowedOrigin.includes(origin)) cors['Access-Control-Allow-Origin']=origin;
  const J=(code,o)=>{if(res.headersSent)return;res.writeHead(code,{...cors,'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(o))};
  if(req.method==='OPTIONS'){if(req.headers.origin && !allowedOrigin.includes(req.headers.origin)){res.writeHead(403);return res.end()}res.writeHead(204,cors);return res.end()}
  const ip=ipOf(req);
  // Reject ambiguous or malformed request framing early; never guess body length.
  const rawLength=req.headers['content-length'];
  if((rawLength!==undefined&&!/^\d+$/.test(String(rawLength)))||(rawLength!==undefined&&req.headers['transfer-encoding'])){
    res.writeHead(400,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Connection':'close'});
    return res.end(JSON.stringify({err:'Некорректные заголовки запроса'}));
  }
  // В production TLS должен завершаться на самом сервере или на явно доверенном прокси.
  // X-Forwarded-Proto учитывается только при настроенном TRUST_PROXY.
  const protoChain=String(req.headers['x-forwarded-proto']||'').split(',').map(x=>x.trim().toLowerCase()).filter(Boolean);
  const forwardedHttps=HOPS>0&&trustedProxy(req)&&protoChain.length>=HOPS&&protoChain.every(x=>x==='https');
  const externalProto=(req.socket.encrypted||forwardedHttps)?'https':'http';
  if(REQUIRE_HTTPS&&!req.socket.encrypted&&!forwardedHttps){res.writeHead(426,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','Strict-Transport-Security':'max-age=31536000; includeSubDomains'});return res.end(JSON.stringify({err:'Требуется защищённое HTTPS-соединение'}))}
  // Защита от CSRF: браузерный запрос с Origin принимается только с этого origin
  // либо из явного ALLOW_ORIGIN. Клиенты без Origin (например, CLI) используют токен.
  if(origin){
    let sameOrigin=false,normalizedOrigin='';
    try{const o=new URL(origin),h=new URL(externalProto+'://'+String(req.headers.host||''));normalizedOrigin=o.origin;sameOrigin=(o.protocol===externalProto+':'&&o.host===h.host&&['http:','https:'].includes(o.protocol))}catch(e){}
    if(!sameOrigin&&!allowedOrigin.includes(normalizedOrigin))return J(403,{err:'Источник запроса не разрешён'});
  }
  if(req.method==='POST'&&url.pathname.startsWith('/api/')){
    // Локальный лимит остаётся активным и без Redis; Redis добавляет общий лимит между процессами.
    if(postRateLimited(ip))return J(429,{err:'Слишком много запросов с этого адреса. Попробуйте позже.'});
    try{if(!(await distributedLimit(url.pathname,ip)))return J(429,{err:'Слишком много запросов. Попробуйте позже.'})}
    catch(e){console.error('Distributed rate limit error:',e.message);return J(503,{err:'Сервис временно недоступен: защита от запросов не готова.'})}
  }
  res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('X-Frame-Options','DENY');res.setHeader('Referrer-Policy','no-referrer');
  res.setHeader('X-Permitted-Cross-Domain-Policies','none');res.setHeader('Cross-Origin-Resource-Policy','same-origin');
  res.setHeader('Permissions-Policy','camera=(self), microphone=(self), geolocation=()');res.setHeader('Cross-Origin-Opener-Policy','same-origin');res.setHeader('Origin-Agent-Cluster','?1');
  if(req.socket.encrypted||(HOPS>0&&forwardedHttps))res.setHeader('Strict-Transport-Security','max-age=31536000; includeSubDomains');
  try{
    if(url.pathname==='/api/ping')return J(200,{ok:1});
    if(url.pathname==='/api/stream'){
      const ticket=url.searchParams.get('ticket'),grant=STREAM_TICKETS.get(ticket);if(ticket)STREAM_TICKETS.delete(ticket);
      const now=Date.now(),meta=grant&&DB.tokenMeta[grant.u],stored=grant&&DB.tokens[grant.u];
      /* Consume-once + session binding + inactivity check. A ticket cannot outlive logout,
         password/recovery rotation, account deletion, or the session's idle deadline. */
      if(!grant||grant.exp<=now||!DB.users[grant.u]||typeof stored!=='string'||typeof grant.sessionHash!=='string'||!ceq(stored,grant.sessionHash)||!meta||!Number.isFinite(meta.exp)||meta.exp<=now||!Number.isFinite(meta.last)||meta.last>now||now-meta.last>=SESSION_IDLE_MAX_AGE)return J(401,{err:'Подключение истекло. Переподключитесь.'});
      const u=grant.u,ip=ipOf(req);
      /* Bound live SSE sockets per account AND source IP. Account-only limits can
         still be bypassed by creating many accounts from one host/network. */
      if(clients.size>=5000||(streamCountByUser.get(u)||0)>=6||(streamCountByIP.get(ip)||0)>=30)return J(429,{err:'Слишком много подключений'});
      res.writeHead(200,{...cors,'Content-Type':'text/event-stream','Cache-Control':'no-store, no-cache, no-transform','Connection':'keep-alive','X-Accel-Buffering':'no'});
      const cl={res,u,ip,ka:null};clients.add(cl);bumpCount(streamCountByUser,u,1);bumpCount(streamCountByIP,ip,1);send(cl,{t:'init',s:state(u)});send(cl,{t:'bal',v:DB.bal[u]||0});
      cl.ka=setInterval(()=>{const sm=DB.tokenMeta[u],now=Date.now(),current=DB.tokens[u];if(!clients.has(cl)||!sm||sm.exp<=now||!Number.isFinite(sm.last)||sm.last>now||now-sm.last>=SESSION_IDLE_MAX_AGE||typeof current!=='string'||!ceq(current,grant.sessionHash)){removeClient(cl);try{res.end()}catch(e){}return}try{res.write(': ka\n\n')}catch(e){removeClient(cl)}},20000);if(!clients.has(cl))clearInterval(cl.ka);req.on('close',()=>removeClient(cl));res.on('close',()=>removeClient(cl));return}
    const ap=/^\/api\/([a-z-]+\/[a-z-]+)$/.exec(url.pathname);
    if(ap&&req.method==='POST'&&API[ap[1]]){if(!/^application\/json(?:\s*;|$)/i.test(String(req.headers['content-type']||'')))return J(415,{err:'Ожидается Content-Type: application/json'});const b=jp((await body(req))||'{}'),h=API[ap[1]];let u=null;if(!b||typeof b!=='object')return J(400,{err:'Неверный запрос'});if(!h.open){if(!auth(b.u,b.t,ip))return J(401,{err:'Нет доступа'});u=b.u}return J(200,await h.f(u,b,req))}
    if(url.pathname==='/api/w'&&req.method==='POST'){
      if(!/^application\/json(?:\s*;|$)/i.test(String(req.headers['content-type']||'')))return J(415,{err:'Ожидается Content-Type: application/json'});
      const{u,t,op,c,id,v}=jp(await body(req))||{};
      if(!auth(u,t,ip))return J(401,{err:'Нет доступа'});
      if(locked('w:'+u,300,6e4))return J(429,{err:'Слишком часто. Подождите минуту.'});fail('w:'+u);
      if(c!=='media'&&JSON.stringify(v||{}).length>4e5)return J(413,{err:'Слишком большой объект'});
      if(typeof id!=='string'||!/^[A-Za-z0-9_\-]{1,100}$/.test(id)||RESERVED.has(id))return J(400,{err:'Неверный id'});
      if(c==='media'){
        if(op!=='set'||!v||!validImageDataUrl(v.d))return J(400,{err:'Неверный файл: содержимое не соответствует формату PNG/JPEG/WebP/GIF'});
        const f=path.join(MEDIA,id),Q=(+process.env.MEDIA_QUOTA_MB||200)*1e6,size=v.d.length;
        if(fs.existsSync(f)||MEDIA_PENDING_IDS.has(id))return J(409,{err:'Уже есть'});
        if((DB.mq[u]||0)+(MEDIA_PENDING_BYTES[u]||0)+size>Q)return J(413,{err:'Достигнут лимит хранилища медиа'});
        MEDIA_PENDING_IDS.add(id);MEDIA_PENDING_BYTES[u]=(MEDIA_PENDING_BYTES[u]||0)+size;
        return fs.writeFile(f,v.d,{mode:0o600},e=>{
          MEDIA_PENDING_IDS.delete(id);MEDIA_PENDING_BYTES[u]=Math.max(0,(MEDIA_PENDING_BYTES[u]||0)-size);if(!MEDIA_PENDING_BYTES[u])delete MEDIA_PENDING_BYTES[u];
          if(e)return J(500,{err:'Ошибка диска'});
          // The account may have been deleted or the session revoked while the disk write was pending.
          if(!DB.users[u]||!DB.tokenMeta[u]||DB.tokenMeta[u].exp<=Date.now()||!Number.isFinite(DB.tokenMeta[u].last)||DB.tokenMeta[u].last>Date.now()||Date.now()-DB.tokenMeta[u].last>=SESSION_IDLE_MAX_AGE||!ceq(DB.tokens[u]||'',hash(t))){try{fs.unlinkSync(f)}catch(_){}return J(401,{err:'Сессия больше не действительна'})}
          DB.mq[u]=(DB.mq[u]||0)+size;DB.mediaOwner[id]=u;save();J(200,{ok:1})
        })
      }
      if(!COLS.includes(c)||!['set','update','delete'].includes(op))return J(400,{err:'Неверная операция'});
      // New direct messages are rate-limited per account and per conversation.
      // This is server-side (not UI-only) and also covers update-on-missing-ID.
      if(c==='msgs'&&(op==='set'||op==='update')&&!DB.msgs[id]){
        const candidate=op==='update'?Object.assign({},v||{}):v;
        if(validMessageShape(candidate)&&candidate.from===u){
          const cv=String(candidate.c||''),g=DB.groups[cv];let target='';
          if(g&&isMem(g,u)&&(!g.ch||isAdm(g,u)))target='group:'+cv;
          else{const ps=cv.split('_');if(ps.length===2&&ps[0]!==ps[1]&&cv===ps.slice().sort().join('_')&&ps.every(x=>DB.users[x])&&!ps.includes('bot')&&ps.includes(u))target='dm:'+(ps[0]===u?ps[1]:ps[0]);}
          // Invalid conversation IDs do not allocate arbitrary limiter keys.
          if(target){const accountKey='msg-send:'+u,conversationKey='msg-peer:'+u+':'+target;
            if(locked(accountKey,40,60000)||locked(conversationKey,15,60000))return J(429,{err:'Слишком много сообщений. Подождите немного.'});
            fail(accountKey);fail(conversationKey);
          }
        }
      }
      apply(u,op,c,id,v);return J(200,{ok:1})}
    const m=/^\/api\/doc\/media\/([A-Za-z0-9_\-]{1,100})$/.exec(url.pathname);
    if(m){
      if(req.method!=='GET'){res.setHeader('Allow','GET');return J(405,{err:'Метод не разрешён'})}
      // Never accept session tokens in URLs: URLs can leak into proxy/access logs and diagnostics.
      const mu=url.searchParams.get('u'),match=/^Bearer ([A-Za-z0-9_-]{32,128})$/.exec(String(req.headers.authorization||'')),mt=match&&match[1];
      if(!mu||!mt||!auth(mu,mt,ip))return J(401,{err:'Нет доступа'});
      if(DB.mediaOwner[m[1]]!==mu)return J(404,{err:'Нет файла'});
      return fs.readFile(path.join(MEDIA,m[1]),(e,d)=>{if(e)return J(404,{err:'Нет файла'});res.writeHead(200,{...cors,'Content-Type':'application/json; charset=utf-8','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff','Cross-Origin-Resource-Policy':'same-origin'});res.end(JSON.stringify({d:d.toString()}))})
    }
    if(url.pathname==='/'||url.pathname==='/index.html')return fs.readFile(path.join(__dirname,'index.html'),(e,d)=>{if(e){res.writeHead(404);return res.end('index.html не найден рядом с server.js')}res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache','Content-Security-Policy':cspFor(d.toString('utf8'))});res.end(d)});
    J(404,{err:'Не найдено'})
  }catch(e){
    const status=typeof e.code==='number'?e.code:(e instanceof SyntaxError?400:500);
    if(status>=500)console.error('Внутренняя ошибка API:',e&&e.stack||e);
    J(status,{err:status>=500?'Внутренняя ошибка сервера':(e.message||'Неверный запрос')});
  }
});
server.requestTimeout=30000;server.headersTimeout=15000;server.keepAliveTimeout=5000;server.maxHeadersCount=50;server.maxRequestsPerSocket=1000;
/* Bound TCP sockets before HTTP parsing, so slow/incomplete headers cannot consume
   unlimited descriptors from one peer. Count the actual TCP peer, never spoofable XFF.
   If behind a reverse proxy, tune MAX_TCP_CONNECTIONS_PER_IP for that proxy's fan-in. */
const MAX_TCP_CONNECTIONS_PER_IP=Math.max(20,Math.min(2000,Math.floor(Number(process.env.MAX_TCP_CONNECTIONS_PER_IP)||200)));
const tcpSocketsByIP=new Map();
server.maxConnections=5500;
server.on('connection',socket=>{
  const peer=normalizeIP(socket.remoteAddress||'unknown'),count=tcpSocketsByIP.get(peer)||0;
  if(count>=MAX_TCP_CONNECTIONS_PER_IP){socket.destroy();return}
  tcpSocketsByIP.set(peer,count+1);
  let released=false;
  socket.once('close',()=>{if(released)return;released=true;const n=(tcpSocketsByIP.get(peer)||1)-1;if(n>0)tcpSocketsByIP.set(peer,n);else tcpSocketsByIP.delete(peer)});
});
process.on('uncaughtException',e=>console.error('uncaughtException:',e));process.on('unhandledRejection',e=>console.error('unhandledRejection:',e));
server.listen(PORT,()=>console.log('Сириус запущен: http://localhost:'+PORT+'  (данные: '+DIR+')'));

/* сохранить данные при остановке/перезапуске хостинга */
const flush=()=>{try{fs.writeFileSync(FILE+'.tmp',JSON.stringify(DB));fs.renameSync(FILE+'.tmp',FILE)}catch(e){}process.exit(0)};
process.on('SIGTERM',flush);process.on('SIGINT',flush);
