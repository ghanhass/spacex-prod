var Tx=Object.defineProperty,Ax=Object.defineProperties;var Rx=Object.getOwnPropertyDescriptors;var sy=Object.getOwnPropertySymbols;var kx=Object.prototype.hasOwnProperty,Nx=Object.prototype.propertyIsEnumerable;var ay=(e,n,t)=>n in e?Tx(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,_=(e,n)=>{for(var t in n||={})kx.call(n,t)&&ay(e,t,n[t]);if(sy)for(var t of sy(n))Nx.call(n,t)&&ay(e,t,n[t]);return e},A=(e,n)=>Ax(e,Rx(n));var Se=(e,n,t)=>new Promise((i,r)=>{var o=c=>{try{a(t.next(c))}catch(l){r(l)}},s=c=>{try{a(t.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((t=t.apply(e,n)).next())});var pt=null,_c=!1,Af=1,Ox=null,qe=Symbol("SIGNAL");function O(e){let n=pt;return pt=e,n}function Cc(){return pt}var Wi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function mi(e){if(_c)throw new Error("");if(pt===null)return;pt.consumerOnSignalRead(e);let n=pt.producersTail;if(n!==void 0&&n.producer===e)return;let t,i=pt.recomputing;if(i&&(t=n!==void 0?n.nextProducer:pt.producers,t!==void 0&&t.producer===e)){pt.producersTail=t,t.lastReadVersion=e.version;return}let r=e.consumersTail;if(r!==void 0&&r.consumer===pt&&(!i||Px(r,pt)))return;let o=qr(pt),s={producer:e,consumer:pt,nextProducer:t,prevConsumer:r,lastReadVersion:e.version,nextConsumer:void 0};pt.producersTail=s,n!==void 0?n.nextProducer=s:pt.producers=s,o&&uy(e,s)}function cy(){Af++}function Dc(e){if(!(qr(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===Af)){if(!e.producerMustRecompute(e)&&!Wr(e)){Ec(e);return}e.producerRecomputeValue(e),Ec(e)}}function Rf(e){if(e.consumers===void 0)return;let n=_c;_c=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||Fx(i)}}finally{_c=n}}function kf(){return pt?.consumerAllowSignalWrites!==!1}function Fx(e){e.dirty=!0,Rf(e),e.consumerMarkedDirty?.(e)}function Ec(e){e.dirty=!1,e.lastCleanEpoch=Af}function gi(e){return e&&ly(e),O(e)}function ly(e){e.producersTail=void 0,e.recomputing=!0}function qi(e,n){O(n),e&&dy(e)}function dy(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(qr(e))do t=Nf(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function Wr(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,i=n.lastReadVersion;if(i!==t.version||(Dc(t),i!==t.version))return!0}return!1}function vi(e){if(qr(e)){let n=e.producers;for(;n!==void 0;)n=Nf(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function uy(e,n){let t=e.consumersTail,i=qr(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!i)for(let r=e.producers;r!==void 0;r=r.nextProducer)uy(r.producer,r)}function Nf(e){let n=e.producer,t=e.nextProducer,i=e.nextConsumer,r=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!qr(n)){let o=n.producers;for(;o!==void 0;)o=Nf(o)}return t}function qr(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function Ic(e){Ox?.(e)}function Px(e,n){let t=n.producersTail;if(t!==void 0){let i=n.producers;do{if(i===e)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function xc(e,n){return Object.is(e,n)}function ls(e,n){let t=Object.create(Lx);t.computation=e,n!==void 0&&(t.equal=n);let i=()=>{if(Dc(t),mi(t),t.value===cs)throw t.error;return t.value};return i[qe]=t,Ic(t),i}var bc=Symbol("UNSET"),wc=Symbol("COMPUTING"),cs=Symbol("ERRORED"),Lx=A(_({},Wi),{value:bc,dirty:!0,error:null,equal:xc,kind:"computed",producerMustRecompute(e){return e.value===bc||e.value===wc},producerRecomputeValue(e){if(e.value===wc)throw new Error("");let n=e.value;e.value=wc;let t=gi(e),i,r=!1;try{i=e.computation(),O(null),r=n!==bc&&n!==cs&&i!==cs&&e.equal(n,i)}catch(o){i=cs,e.error=o}finally{qi(e,t)}if(r){e.value=n;return}e.value=i,e.version++}});function jx(){throw new Error}var fy=jx;function hy(e){fy(e)}function Of(e){fy=e}var Vx=null;function Ff(e,n){let t=Object.create(ds);t.value=e,n!==void 0&&(t.equal=n);let i=()=>py(t);return i[qe]=t,Ic(t),[i,s=>Yi(t,s),s=>Pf(t,s)]}function py(e){return mi(e),e.value}function Yi(e,n){kf()||hy(e),e.equal(e.value,n)||(e.value=n,Bx(e))}function Pf(e,n){kf()||hy(e),Yi(e,n(e.value))}var ds=A(_({},Wi),{equal:xc,value:void 0,kind:"signal"});function Bx(e){e.version++,cy(),Rf(e),Vx?.(e)}var Lf=A(_({},Wi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function jf(e){if(e.dirty=!1,e.version>0&&!Wr(e))return;e.version++;let n=gi(e);try{e.cleanup(),e.fn()}finally{qi(e,n)}}function Y(e){return typeof e=="function"}function yi(e){let t=e(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Sc=yi(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Zi(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var Ie=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Y(i))try{i()}catch(o){n=o instanceof Sc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{my(o)}catch(s){n=n??[],s instanceof Sc?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Sc(n)}}add(n){var t;if(n&&n!==this)if(this.closed)my(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&Zi(t,n)}remove(n){let{_finalizers:t}=this;t&&Zi(t,n),n instanceof e&&n._removeParent(this)}};Ie.EMPTY=(()=>{let e=new Ie;return e.closed=!0,e})();var Vf=Ie.EMPTY;function Mc(e){return e instanceof Ie||e&&"closed"in e&&Y(e.remove)&&Y(e.add)&&Y(e.unsubscribe)}function my(e){Y(e)?e():e.unsubscribe()}var an={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Yr={setTimeout(e,n,...t){let{delegate:i}=Yr;return i?.setTimeout?i.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=Yr;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function Tc(e){Yr.setTimeout(()=>{let{onUnhandledError:n}=an;if(n)n(e);else throw e})}function zn(){}var gy=Bf("C",void 0,void 0);function vy(e){return Bf("E",void 0,e)}function yy(e){return Bf("N",e,void 0)}function Bf(e,n,t){return{kind:e,value:n,error:t}}var Ki=null;function Zr(e){if(an.useDeprecatedSynchronousErrorHandling){let n=!Ki;if(n&&(Ki={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:i}=Ki;if(Ki=null,t)throw i}}else e()}function _y(e){an.useDeprecatedSynchronousErrorHandling&&Ki&&(Ki.errorThrown=!0,Ki.error=e)}var Qi=class extends Ie{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Mc(n)&&n.add(this)):this.destination=$x}static create(n,t,i){return new Gn(n,t,i)}next(n){this.isStopped?Hf(yy(n),this):this._next(n)}error(n){this.isStopped?Hf(vy(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Hf(gy,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Ux=Function.prototype.bind;function Uf(e,n){return Ux.call(e,n)}var $f=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(i){Ac(i)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(i){Ac(i)}else Ac(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){Ac(t)}}},Gn=class extends Qi{constructor(n,t,i){super();let r;if(Y(n)||!n)r={next:n??void 0,error:t??void 0,complete:i??void 0};else{let o;this&&an.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Uf(n.next,o),error:n.error&&Uf(n.error,o),complete:n.complete&&Uf(n.complete,o)}):r=n}this.destination=new $f(r)}};function Ac(e){an.useDeprecatedSynchronousErrorHandling?_y(e):Tc(e)}function Hx(e){throw e}function Hf(e,n){let{onStoppedNotification:t}=an;t&&Yr.setTimeout(()=>t(e,n))}var $x={closed:!0,next:zn,error:Hx,complete:zn};var Kr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function wt(e){return e}function zf(...e){return Gf(e)}function Gf(e){return e.length===0?wt:e.length===1?e[0]:function(t){return e.reduce((i,r)=>r(i),t)}}var H=(()=>{class e{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new e;return i.source=this,i.operator=t,i}subscribe(t,i,r){let o=Gx(t)?t:new Gn(t,i,r);return Zr(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=by(i),new i((r,o)=>{let s=new Gn({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Kr](){return this}pipe(...t){return Gf(t)(this)}toPromise(t){return t=by(t),new t((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return e.create=n=>new e(n),e})();function by(e){var n;return(n=e??an.Promise)!==null&&n!==void 0?n:Promise}function zx(e){return e&&Y(e.next)&&Y(e.error)&&Y(e.complete)}function Gx(e){return e&&e instanceof Qi||zx(e)&&Mc(e)}function Wx(e){return Y(e?.lift)}function F(e){return n=>{if(Wx(n))return n.lift(function(t){try{return e(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function P(e,n,t,i,r){return new us(e,n,t,i,r)}var us=class extends Qi{constructor(n,t,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var wy=yi(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var M=(()=>{class e extends H{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Rc(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new wy}next(t){Zr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Zr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Zr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Vf:(this.currentObservers=null,o.push(t),new Ie(()=>{this.currentObservers=null,Zi(o,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:o}=this;i?t.error(r):o&&t.complete()}asObservable(){let t=new H;return t.source=this,t}}return e.create=(n,t)=>new Rc(n,t),e})(),Rc=class extends M{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,n)}error(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&i!==void 0?i:Vf}};var Pe=class extends M{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:i}=this;if(n)throw t;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var fs={now(){return(fs.delegate||Date).now()},delegate:void 0};var Xi=class extends M{constructor(n=1/0,t=1/0,i=fs){super(),this._bufferSize=n,this._windowTime=t,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=t===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,t)}next(n){let{isStopped:t,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;t||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let t=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),t}_trimBuffer(){let{_bufferSize:n,_timestampProvider:t,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=t.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var kc=class extends Ie{constructor(n,t){super()}schedule(n,t=0){return this}};var hs={setInterval(e,n,...t){let{delegate:i}=hs;return i?.setInterval?i.setInterval(e,n,...t):setInterval(e,n,...t)},clearInterval(e){let{delegate:n}=hs;return(n?.clearInterval||clearInterval)(e)},delegate:void 0};var Qr=class extends kc{constructor(n,t){super(n,t),this.scheduler=n,this.work=t,this.pending=!1}schedule(n,t=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,t)),this.pending=!0,this.delay=t,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,t),this}requestAsyncId(n,t,i=0){return hs.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,t,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return t;t!=null&&hs.clearInterval(t)}execute(n,t){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,t);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,t){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:t}=this,{actions:i}=t;this.work=this.state=this.scheduler=null,this.pending=!1,Zi(i,this),n!=null&&(this.id=this.recycleAsyncId(t,n,null)),this.delay=null,super.unsubscribe()}}};var Xr=class e{constructor(n,t=e.now){this.schedulerActionCtor=n,this.now=t}schedule(n,t=0,i){return new this.schedulerActionCtor(this,n).schedule(i,t)}};Xr.now=fs.now;var Jr=class extends Xr{constructor(n,t=Xr.now){super(n,t),this.actions=[],this._active=!1}flush(n){let{actions:t}=this;if(this._active){t.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=t.shift());if(this._active=!1,i){for(;n=t.shift();)n.unsubscribe();throw i}}};var Nc=new Jr(Qr);var Oc=class extends Qr{constructor(n,t){super(n,t),this.scheduler=n,this.work=t}schedule(n,t=0){return t>0?super.schedule(n,t):(this.delay=t,this.state=n,this.scheduler.flush(this),this)}execute(n,t){return t>0||this.closed?super.execute(n,t):this._execute(n,t)}requestAsyncId(n,t,i=0){return i!=null&&i>0||i==null&&this.delay>0?super.requestAsyncId(n,t,i):(n.flush(this),0)}};var Fc=class extends Jr{};var ps=new Fc(Oc);var xe=new H(e=>e.complete());function Ey(e){return e&&Y(e.schedule)}function Wf(e){return e[e.length-1]}function eo(e){return Y(Wf(e))?e.pop():void 0}function In(e){return Ey(Wf(e))?e.pop():void 0}function Cy(e,n){return typeof Wf(e)=="number"?e.pop():n}function Iy(e,n,t,i){function r(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(u){s(u)}}function c(d){try{l(i.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(e,n||[])).next())})}function Dy(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],i=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&i>=e.length&&(e=void 0),{value:e&&e[i++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Ji(e){return this instanceof Ji?(this.v=e,this):new Ji(e)}function xy(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(e,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(p){return function(m){return Promise.resolve(m).then(p,u)}}function a(p,m){i[p]&&(r[p]=function(b){return new Promise(function(D,S){o.push([p,b,D,S])>1||c(p,b)})},m&&(r[p]=m(r[p])))}function c(p,m){try{l(i[p](m))}catch(b){h(o[0][3],b)}}function l(p){p.value instanceof Ji?Promise.resolve(p.value.v).then(d,u):h(o[0][2],p)}function d(p){c("next",p)}function u(p){c("throw",p)}function h(p,m){p(m),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Sy(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof Dy=="function"?Dy(e):e[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(o){t[o]=e[o]&&function(s){return new Promise(function(a,c){s=e[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Pc=e=>e&&typeof e.length=="number"&&typeof e!="function";function Lc(e){return Y(e?.then)}function jc(e){return Y(e[Kr])}function Vc(e){return Symbol.asyncIterator&&Y(e?.[Symbol.asyncIterator])}function Bc(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function qx(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Uc=qx();function Hc(e){return Y(e?.[Uc])}function $c(e){return xy(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:i,done:r}=yield Ji(t.read());if(r)return yield Ji(void 0);yield yield Ji(i)}}finally{t.releaseLock()}})}function zc(e){return Y(e?.getReader)}function ae(e){if(e instanceof H)return e;if(e!=null){if(jc(e))return Yx(e);if(Pc(e))return Zx(e);if(Lc(e))return Kx(e);if(Vc(e))return My(e);if(Hc(e))return Qx(e);if(zc(e))return Xx(e)}throw Bc(e)}function Yx(e){return new H(n=>{let t=e[Kr]();if(Y(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Zx(e){return new H(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function Kx(e){return new H(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,Tc)})}function Qx(e){return new H(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function My(e){return new H(n=>{Jx(e,n).catch(t=>n.error(t))})}function Xx(e){return My($c(e))}function Jx(e,n){var t,i,r,o;return Iy(this,void 0,void 0,function*(){try{for(t=Sy(e);i=yield t.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=t.return)&&(yield o.call(t))}finally{if(r)throw r.error}}n.complete()})}function mt(e,n,t,i=0,r=!1){let o=n.schedule(function(){t(),r?e.add(this.schedule(null,i)):this.unsubscribe()},i);if(e.add(o),!r)return o}function Wn(e,n=0){return F((t,i)=>{t.subscribe(P(i,r=>mt(i,e,()=>i.next(r),n),()=>mt(i,e,()=>i.complete(),n),r=>mt(i,e,()=>i.error(r),n)))})}function Gc(e,n=0){return F((t,i)=>{i.add(e.schedule(()=>t.subscribe(i),n))})}function Ty(e,n){return ae(e).pipe(Gc(n),Wn(n))}function Ay(e,n){return ae(e).pipe(Gc(n),Wn(n))}function Ry(e,n){return new H(t=>{let i=0;return n.schedule(function(){i===e.length?t.complete():(t.next(e[i++]),t.closed||this.schedule())})})}function ky(e,n){return new H(t=>{let i;return mt(t,n,()=>{i=e[Uc](),mt(t,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){t.error(s);return}o?t.complete():t.next(r)},0,!0)}),()=>Y(i?.return)&&i.return()})}function Wc(e,n){if(!e)throw new Error("Iterable cannot be null");return new H(t=>{mt(t,n,()=>{let i=e[Symbol.asyncIterator]();mt(t,n,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function Ny(e,n){return Wc($c(e),n)}function Oy(e,n){if(e!=null){if(jc(e))return Ty(e,n);if(Pc(e))return Ry(e,n);if(Lc(e))return Ay(e,n);if(Vc(e))return Wc(e,n);if(Hc(e))return ky(e,n);if(zc(e))return Ny(e,n)}throw Bc(e)}function ke(e,n){return n?Oy(e,n):ae(e)}function R(...e){let n=In(e);return ke(e,n)}function er(e,n){let t=Y(e)?e:()=>e,i=r=>r.error(t());return new H(n?r=>n.schedule(i,0,r):i)}var _i=class e{constructor(n,t,i){this.kind=n,this.value=t,this.error=i,this.hasValue=n==="N"}observe(n){return qf(this,n)}do(n,t,i){let{kind:r,value:o,error:s}=this;return r==="N"?n?.(o):r==="E"?t?.(s):i?.()}accept(n,t,i){var r;return Y((r=n)===null||r===void 0?void 0:r.next)?this.observe(n):this.do(n,t,i)}toObservable(){let{kind:n,value:t,error:i}=this,r=n==="N"?R(t):n==="E"?er(()=>i):n==="C"?xe:0;if(!r)throw new TypeError(`Unexpected notification kind ${n}`);return r}static createNext(n){return new e("N",n)}static createError(n){return new e("E",void 0,n)}static createComplete(){return e.completeNotification}};_i.completeNotification=new _i("C");function qf(e,n){var t,i,r;let{kind:o,value:s,error:a}=e;if(typeof o!="string")throw new TypeError('Invalid notification, missing "kind"');o==="N"?(t=n.next)===null||t===void 0||t.call(n,s):o==="E"?(i=n.error)===null||i===void 0||i.call(n,a):(r=n.complete)===null||r===void 0||r.call(n)}function qc(e){return!!e&&(e instanceof H||Y(e.lift)&&Y(e.subscribe))}var tr=yi(e=>function(){e(this),this.name="EmptyError",this.message="no elements in sequence"});function Fy(e){return e instanceof Date&&!isNaN(e)}var eS=yi(e=>function(t=null){e(this),this.message="Timeout has occurred",this.name="TimeoutError",this.info=t});function Yf(e,n){let{first:t,each:i,with:r=tS,scheduler:o=n??Nc,meta:s=null}=Fy(e)?{first:e}:typeof e=="number"?{each:e}:e;if(t==null&&i==null)throw new TypeError("No timeout provided.");return F((a,c)=>{let l,d,u=null,h=0,p=m=>{d=mt(c,o,()=>{try{l.unsubscribe(),ae(r({meta:s,lastValue:u,seen:h})).subscribe(c)}catch(b){c.error(b)}},m)};l=a.subscribe(P(c,m=>{d?.unsubscribe(),h++,c.next(u=m),i>0&&p(i)},void 0,void 0,()=>{d?.closed||d?.unsubscribe(),u=null})),!h&&p(t!=null?typeof t=="number"?t:+t-o.now():i)})}function tS(e){throw new eS(e)}function T(e,n){return F((t,i)=>{let r=0;t.subscribe(P(i,o=>{i.next(e.call(n,o,r++))}))})}var{isArray:nS}=Array;function iS(e,n){return nS(n)?e(...n):e(n)}function Yc(e){return T(n=>iS(e,n))}var{isArray:rS}=Array,{getPrototypeOf:oS,prototype:sS,keys:aS}=Object;function Zc(e){if(e.length===1){let n=e[0];if(rS(n))return{args:n,keys:null};if(cS(n)){let t=aS(n);return{args:t.map(i=>n[i]),keys:t}}}return{args:e,keys:null}}function cS(e){return e&&typeof e=="object"&&oS(e)===sS}function Kc(e,n){return e.reduce((t,i,r)=>(t[i]=n[r],t),{})}function ms(...e){let n=In(e),t=eo(e),{args:i,keys:r}=Zc(e);if(i.length===0)return ke([],n);let o=new H(lS(i,n,r?s=>Kc(r,s):wt));return t?o.pipe(Yc(t)):o}function lS(e,n,t=wt){return i=>{Py(n,()=>{let{length:r}=e,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)Py(n,()=>{let l=ke(e[c],n),d=!1;l.subscribe(P(i,u=>{o[c]=u,d||(d=!0,a--),a||i.next(t(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Py(e,n,t){e?mt(t,e,n):n()}function Ly(e,n,t,i,r,o,s,a){let c=[],l=0,d=0,u=!1,h=()=>{u&&!c.length&&!l&&n.complete()},p=b=>l<i?m(b):c.push(b),m=b=>{o&&n.next(b),l++;let D=!1;ae(t(b,d++)).subscribe(P(n,S=>{r?.(S),o?p(S):n.next(S)},()=>{D=!0},void 0,()=>{if(D)try{for(l--;c.length&&l<i;){let S=c.shift();s?mt(n,s,()=>m(S)):m(S)}h()}catch(S){n.error(S)}}))};return e.subscribe(P(n,p,()=>{u=!0,h()})),()=>{a?.()}}function Xe(e,n,t=1/0){return Y(n)?Xe((i,r)=>T((o,s)=>n(i,o,r,s))(ae(e(i,r))),t):(typeof n=="number"&&(t=n),F((i,r)=>Ly(i,r,e,t)))}function Qc(e=1/0){return Xe(wt,e)}function jy(){return Qc(1)}function bi(...e){return jy()(ke(e,In(e)))}function gs(e){return new H(n=>{ae(e()).subscribe(n)})}function vs(...e){let n=eo(e),{args:t,keys:i}=Zc(e),r=new H(o=>{let{length:s}=t;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;ae(t[d]).subscribe(P(o,h=>{u||(u=!0,l--),a[d]=h},()=>c--,void 0,()=>{(!c||!u)&&(l||o.next(i?Kc(i,a):a),o.complete())}))}});return n?r.pipe(Yc(n)):r}function jt(...e){let n=In(e),t=Cy(e,1/0),i=e;return i.length?i.length===1?ae(i[0]):Qc(t)(ke(i,n)):xe}function _e(e,n){return F((t,i)=>{let r=0;t.subscribe(P(i,o=>e.call(n,o,r++)&&i.next(o)))})}function Et(e){return F((n,t)=>{let i=null,r=!1,o;i=n.subscribe(P(t,void 0,void 0,s=>{o=ae(e(s,Et(e)(n))),i?(i.unsubscribe(),i=null,o.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(t))})}function Vy(e,n,t,i,r){return(o,s)=>{let a=t,c=n,l=0;o.subscribe(P(s,d=>{let u=l++;c=a?e(c,d,u):(a=!0,d),i&&s.next(c)},r&&(()=>{a&&s.next(c),s.complete()})))}}function wi(e,n){return Y(n)?Xe(e,n,1):Xe(e,1)}function nr(e,n=Nc){return F((t,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+e,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}t.subscribe(P(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,e),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function By(e){return F((n,t)=>{let i=!1;n.subscribe(P(t,r=>{i=!0,t.next(r)},()=>{i||t.next(e),t.complete()}))})}function ct(e){return e<=0?()=>xe:F((n,t)=>{let i=0;n.subscribe(P(t,r=>{++i<=e&&(t.next(r),e<=i&&t.complete())}))})}function Zf(){return F((e,n)=>{e.subscribe(P(n,zn))})}function Kf(){return F((e,n)=>{e.subscribe(P(n,t=>qf(t,n)))})}function ys(e,n=wt){return e=e??dS,F((t,i)=>{let r,o=!0;t.subscribe(P(i,s=>{let a=n(s);(o||!e(r,a))&&(o=!1,r=a,i.next(s))}))})}function dS(e,n){return e===n}function Uy(e=uS){return F((n,t)=>{let i=!1;n.subscribe(P(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(e())))})}function uS(){return new tr}function Ei(e,n){return n?t=>t.pipe(Ei((i,r)=>ae(e(i,r)).pipe(T((o,s)=>n(i,o,r,s))))):F((t,i)=>{let r=0,o=null,s=!1;t.subscribe(P(i,a=>{o||(o=P(i,void 0,()=>{o=null,s&&i.complete()}),ae(e(a,r++)).subscribe(o))},()=>{s=!0,!o&&i.complete()}))})}function ir(e){return F((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function qn(e,n){let t=arguments.length>=2;return i=>i.pipe(e?_e((r,o)=>e(r,o,i)):wt,ct(1),t?By(n):Uy(()=>new tr))}function Xc(e,n,t,i){return F((r,o)=>{let s;!n||typeof n=="function"?s=n:{duration:t,element:s,connector:i}=n;let a=new Map,c=m=>{a.forEach(m),m(o)},l=m=>c(b=>b.error(m)),d=0,u=!1,h=new us(o,m=>{try{let b=e(m),D=a.get(b);if(!D){a.set(b,D=i?i():new M);let S=p(b,D);if(o.next(S),t){let L=P(D,()=>{D.complete(),L?.unsubscribe()},void 0,void 0,()=>a.delete(b));h.add(ae(t(S)).subscribe(L))}}D.next(s?s(m):m)}catch(b){l(b)}},()=>c(m=>m.complete()),l,()=>a.clear(),()=>(u=!0,d===0));r.subscribe(h);function p(m,b){let D=new H(S=>{d++;let L=b.subscribe(S);return()=>{L.unsubscribe(),--d===0&&u&&h.unsubscribe()}});return D.key=m,D}})}function Jc(e){return e<=0?()=>xe:F((n,t)=>{let i=[];n.subscribe(P(t,r=>{i.push(r),e<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Qf(){return F((e,n)=>{e.subscribe(P(n,t=>{n.next(_i.createNext(t))},()=>{n.next(_i.createComplete()),n.complete()},t=>{n.next(_i.createError(t)),n.complete()}))})}function Xf(){return F((e,n)=>{let t,i=!1;e.subscribe(P(n,r=>{let o=t;t=r,i&&n.next([o,r]),i=!0}))})}function Jf(...e){let n=e.length;if(n===0)throw new Error("list of properties cannot be empty.");return T(t=>{let i=t;for(let r=0;r<n;r++){let o=i?.[e[r]];if(typeof o<"u")i=o;else return}return i})}function _s(e,n){return F(Vy(e,n,arguments.length>=2,!0))}function rr(e={}){let{connector:n=()=>new M,resetOnError:t=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=e;return o=>{let s,a,c,l=0,d=!1,u=!1,h=()=>{a?.unsubscribe(),a=void 0},p=()=>{h(),s=c=void 0,d=u=!1},m=()=>{let b=s;p(),b?.unsubscribe()};return F((b,D)=>{l++,!u&&!d&&h();let S=c=c??n();D.add(()=>{l--,l===0&&!u&&!d&&(a=eh(m,r))}),S.subscribe(D),!s&&l>0&&(s=new Gn({next:L=>S.next(L),error:L=>{u=!0,h(),a=eh(p,t,L),S.error(L)},complete:()=>{d=!0,h(),a=eh(p,i),S.complete()}}),ae(b).subscribe(s))})(o)}}function eh(e,n,...t){if(n===!0){e();return}if(n===!1)return;let i=new Gn({next:()=>{i.unsubscribe(),e()}});return ae(n(...t)).subscribe(i)}function th(e,n,t){let i,r=!1;return e&&typeof e=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:t}=e:i=e??1/0,rr({connector:()=>new Xi(i,n,t),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function or(e){return _e((n,t)=>e<=t)}function cn(...e){let n=In(e);return F((t,i)=>{(n?bi(e,t,n):bi(e,t)).subscribe(i)})}function He(e,n){return F((t,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();t.subscribe(P(i,c=>{r?.unsubscribe();let l=0,d=o++;ae(e(c,d)).subscribe(r=P(i,u=>i.next(n?n(c,u,d,l++):u),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ne(e){return F((n,t)=>{ae(e).subscribe(P(t,()=>t.complete(),zn)),!t.closed&&n.subscribe(t)})}function Je(e,n,t){let i=Y(e)||n||t?{next:e,error:n,complete:t}:e;return i?F((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(P(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):wt}function bs(...e){let n=eo(e);return F((t,i)=>{let r=e.length,o=new Array(r),s=e.map(()=>!1),a=!1;for(let c=0;c<r;c++)ae(e[c]).subscribe(P(i,l=>{o[c]=l,!a&&!s[c]&&(s[c]=!0,(a=s.every(wt))&&(s=null))},zn));t.subscribe(P(i,c=>{if(a){let l=[c,...o];i.next(n?n(...l):l)}}))})}var nh;function el(){return nh}function xn(e){let n=nh;return nh=e,n}var Hy=Symbol("NotFound");function to(e){return e===Hy||e?.name==="\u0275NotFound"}function $y(e){let n=O(null);try{return e()}finally{O(n)}}var al="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",x=class extends Error{code;constructor(n,t){super(Mn(n,t)),this.code=n}};function fS(e){return`NG0${Math.abs(e)}`}function Mn(e,n){return`${fS(e)}${n?": "+n:""}`}var Tn=globalThis;function ge(e){for(let n in e)if(e[n]===ge)return n;throw Error("")}function Yy(e,n){for(let t in n)n.hasOwnProperty(t)&&!e.hasOwnProperty(t)&&(e[t]=n[t])}function Ss(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(Ss).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function cl(e,n){return e?n?`${e} ${n}`:e:n||""}var hS=ge({__forward_ref__:ge});function Vt(e){return e.__forward_ref__=Vt,e}function lt(e){return gh(e)?e():e}function gh(e){return typeof e=="function"&&e.hasOwnProperty(hS)&&e.__forward_ref__===Vt}function E(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function Z(e){return{providers:e.providers||[],imports:e.imports||[]}}function Ms(e){return pS(e,ll)}function vh(e){return Ms(e)!==null}function pS(e,n){return e.hasOwnProperty(n)&&e[n]||null}function mS(e){let n=e?.[ll]??null;return n||null}function rh(e){return e&&e.hasOwnProperty(nl)?e[nl]:null}var ll=ge({\u0275prov:ge}),nl=ge({\u0275inj:ge}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=E({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function yh(e){return e&&!!e.\u0275providers}var _h=ge({\u0275cmp:ge}),bh=ge({\u0275dir:ge}),wh=ge({\u0275pipe:ge}),Eh=ge({\u0275mod:ge}),Es=ge({\u0275fac:ge}),dr=ge({__NG_ELEMENT_ID__:ge}),zy=ge({__NG_ENV_ID__:ge});function Ch(e){return dl(e,"@NgModule"),e[Eh]||null}function Zn(e){return dl(e,"@Component"),e[_h]||null}function Dh(e){return dl(e,"@Directive"),e[bh]||null}function Zy(e){return dl(e,"@Pipe"),e[wh]||null}function dl(e,n){if(e==null)throw new x(-919,!1)}function ul(e){return typeof e=="string"?e:e==null?"":String(e)}var Ky=ge({ngErrorCode:ge}),gS=ge({ngErrorMessage:ge}),vS=ge({ngTokenPath:ge});function Ih(e,n){return Qy("",-200,n)}function fl(e,n){throw new x(-201,!1)}function Qy(e,n,t){let i=new x(n,e);return i[Ky]=n,i[gS]=e,t&&(i[vS]=t),i}function yS(e){return e[Ky]}var oh;function Xy(){return oh}function Ct(e){let n=oh;return oh=e,n}function xh(e,n,t){let i=Ms(e);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(n!==void 0)return n;fl(e,"")}var _S={},sr=_S,sh="__NG_DI_FLAG__",ah=class{injector;constructor(n){this.injector=n}retrieve(n,t){let i=ar(t)||0;try{return this.injector.get(n,i&8?null:sr,i)}catch(r){if(to(r))return r;throw r}}};function bS(e,n=0){let t=el();if(t===void 0)throw new x(-203,!1);if(t===null)return xh(e,void 0,n);{let i=wS(n),r=t.retrieve(e,i);if(to(r)){if(i.optional)return null;throw r}return r}}function I(e,n=0){return(Xy()||bS)(lt(e),n)}function f(e,n){return I(e,ar(n))}function ar(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function wS(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function ch(e){let n=[];for(let t=0;t<e.length;t++){let i=lt(e[t]);if(Array.isArray(i)){if(i.length===0)throw new x(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=ES(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(I(r,o))}else n.push(I(i))}return n}function Jy(e,n){return e[sh]=n,e.prototype[sh]=n,e}function ES(e){return e[sh]}function Ci(e,n){let t=e.hasOwnProperty(Es);return t?e[Es]:null}function e_(e,n,t){if(e.length!==n.length)return!1;for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(t&&(r=t(r),o=t(o)),o!==r)return!1}return!0}function t_(e){return e.flat(Number.POSITIVE_INFINITY)}function hl(e,n){e.forEach(t=>Array.isArray(t)?hl(t,n):n(t))}function Sh(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function Ts(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function n_(e,n){let t=[];for(let i=0;i<e;i++)t.push(n);return t}function i_(e,n,t,i){let r=e.length;if(r==n)e.push(t,i);else if(r===1)e.push(i,e[0]),e[0]=t;else{for(r--,e.push(e[r-1],e[r]);r>n;){let o=r-2;e[r]=e[o],r--}e[n]=t,e[n+1]=i}}function pl(e,n,t){let i=io(e,n);return i>=0?e[i|1]=t:(i=~i,i_(e,i,n,t)),i}function ml(e,n){let t=io(e,n);if(t>=0)return e[t|1]}function io(e,n){return CS(e,n,1)}function CS(e,n,t){let i=0,r=e.length>>t;for(;r!==i;){let o=i+(r-i>>1),s=e[o<<t];if(n===s)return o<<t;s>n?r=o:i=o+1}return~(r<<t)}var xi={},gt=[],Si=new y(""),Mh=new y("",-1),Th=new y(""),Cs=class{get(n,t=sr){if(t===sr){let r=Qy("",-201);throw r.name="\u0275NotFound",r}return t}};function Tt(e){return{\u0275providers:e}}function ro(e){return Tt([{provide:Si,multi:!0,useValue:e}])}function r_(...e){return{\u0275providers:Ah(!0,e),\u0275fromNgModule:!0}}function Ah(e,...n){let t=[],i=new Set,r,o=s=>{t.push(s)};return hl(n,s=>{let a=s;il(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&o_(r,o),t}function o_(e,n){for(let t=0;t<e.length;t++){let{ngModule:i,providers:r}=e[t];Rh(r,o=>{n(o,i)})}}function il(e,n,t,i){if(e=lt(e),!e)return!1;let r=null,o=rh(e),s=!o&&Zn(e);if(!o&&!s){let c=e.ngModule;if(o=rh(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=e}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)il(l,n,t,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;hl(o.imports,d=>{il(d,n,t,i)&&(l||=[],l.push(d))}),l!==void 0&&o_(l,n)}if(!a){let l=Ci(r)||(()=>new r);n({provide:r,useFactory:l,deps:gt},r),n({provide:Th,useValue:r,multi:!0},r),n({provide:Si,useValue:()=>I(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=e;Rh(c,d=>{n(d,l)})}}else return!1;return r!==e&&e.providers!==void 0}function Rh(e,n){for(let t of e)yh(t)&&(t=t.\u0275providers),Array.isArray(t)?Rh(t,n):n(t)}var DS=ge({provide:String,useValue:ge});function s_(e){return e!==null&&typeof e=="object"&&DS in e}function IS(e){return!!(e&&e.useExisting)}function xS(e){return!!(e&&e.useFactory)}function cr(e){return typeof e=="function"}function a_(e){return!!e.useClass}var As=new y(""),tl={},Gy={},ih;function oo(){return ih===void 0&&(ih=new Cs),ih}var Ee=class{},lr=class extends Ee{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,dh(n,s=>this.processProvider(s)),this.records.set(Mh,no(void 0,this)),r.has("environment")&&this.records.set(Ee,no(void 0,this));let o=this.records.get(As);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Th,gt,{self:!0}))}retrieve(n,t){let i=ar(t)||0;try{return this.get(n,sr,i)}catch(r){if(to(r))return r;throw r}}destroy(){ws(this),this._destroyed=!0;let n=O(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),O(n)}}onDestroy(n){return ws(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ws(this);let t=xn(this),i=Ct(void 0),r;try{return n()}finally{xn(t),Ct(i)}}get(n,t=sr,i){if(ws(this),n.hasOwnProperty(zy))return n[zy](this);let r=ar(i),o,s=xn(this),a=Ct(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=RS(n)&&Ms(n);d&&this.injectableDefInScope(d)?l=no(lh(n),tl):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?oo():this.parent;return t=r&8&&t===sr?null:t,c.get(n,t)}catch(c){let l=yS(c);throw l===-200||l===-201?new x(l,null):c}finally{Ct(a),xn(s)}}resolveInjectorInitializers(){let n=O(null),t=xn(this),i=Ct(void 0),r;try{let o=this.get(Si,gt,{self:!0});for(let s of o)s()}finally{xn(t),Ct(i),O(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=lt(n);let t=cr(n)?n:lt(n&&n.provide),i=MS(n);if(!cr(n)&&n.multi===!0){let r=this.records.get(t);r||(r=no(void 0,tl,!0),r.factory=()=>ch(r.multi),this.records.set(t,r)),t=n,r.multi.push(n)}this.records.set(t,i)}hydrate(n,t,i){let r=O(null);try{if(t.value===Gy)throw Ih("");return t.value===tl&&(t.value=Gy,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&AS(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{O(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=lt(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function lh(e){let n=Ms(e),t=n!==null?n.factory:Ci(e);if(t!==null)return t;if(e instanceof y)throw new x(-204,!1);if(e instanceof Function)return SS(e);throw new x(-204,!1)}function SS(e){if(e.length>0)throw new x(-204,!1);let t=mS(e);return t!==null?()=>t.factory(e):()=>new e}function MS(e){if(s_(e))return no(void 0,e.useValue);{let n=kh(e);return no(n,tl)}}function kh(e,n,t){let i;if(cr(e)){let r=lt(e);return Ci(r)||lh(r)}else if(s_(e))i=()=>lt(e.useValue);else if(xS(e))i=()=>e.useFactory(...ch(e.deps||[]));else if(IS(e))i=(r,o)=>I(lt(e.useExisting),o!==void 0&&o&8?8:void 0);else{let r=lt(e&&(e.useClass||e.provide));if(TS(e))i=()=>new r(...ch(e.deps));else return Ci(r)||lh(r)}return i}function ws(e){if(e.destroyed)throw new x(-205,!1)}function no(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function TS(e){return!!e.deps}function AS(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function RS(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function dh(e,n){for(let t of e)Array.isArray(t)?dh(t,n):t&&yh(t)?dh(t.\u0275providers,n):n(t)}function et(e,n){let t;e instanceof lr?(ws(e),t=e):t=new ah(e);let i,r=xn(t),o=Ct(void 0);try{return n()}finally{xn(r),Ct(o)}}function c_(){return Xy()!==void 0||el()!=null}var ln=0,V=1,$=2,Ke=3,Bt=4,Dt=5,ur=6,so=7,$e=8,Kn=9,dn=10,Me=11,ao=12,Nh=13,fr=14,It=15,Mi=16,hr=17,An=18,Qn=19,Oh=20,Yn=21,gl=22,Di=23,Nt=24,pr=25,Ti=26,Oe=27,l_=1,Fh=6,Ai=7,Rs=8,mr=9,Ve=10;function Xn(e){return Array.isArray(e)&&typeof e[l_]=="object"}function un(e){return Array.isArray(e)&&e[l_]===!0}function Ph(e){return(e.flags&4)!==0}function Jn(e){return e.componentOffset>-1}function ks(e){return(e.flags&1)===1}function Rn(e){return!!e.template}function co(e){return(e[$]&512)!==0}function gr(e){return(e[$]&256)===256}var Lh="svg",d_="math";function Ut(e){for(;Array.isArray(e);)e=e[ln];return e}function jh(e,n){return Ut(n[e])}function fn(e,n){return Ut(n[e.index])}function vl(e,n){return e.data[n]}function yl(e,n){return e[n]}function Vh(e,n,t,i){t>=e.data.length&&(e.data[t]=null,e.blueprint[t]=null),n[t]=i}function Ht(e,n){let t=n[e];return Xn(t)?t:t[ln]}function u_(e){return(e[$]&4)===4}function _l(e){return(e[$]&128)===128}function f_(e){return un(e[Ke])}function $t(e,n){return n==null?null:e[n]}function Bh(e){e[hr]=0}function Uh(e){e[$]&1024||(e[$]|=1024,_l(e)&&vr(e))}function h_(e,n){for(;e>0;)n=n[fr],e--;return n}function Ns(e){return!!(e[$]&9216||e[Nt]?.dirty)}function bl(e){e[dn].changeDetectionScheduler?.notify(8),e[$]&64&&(e[$]|=1024),Ns(e)&&vr(e)}function vr(e){e[dn].changeDetectionScheduler?.notify(0);let n=Ii(e);for(;n!==null&&!(n[$]&8192||(n[$]|=8192,!_l(n)));)n=Ii(n)}function Hh(e,n){if(gr(e))throw new x(911,!1);e[Yn]===null&&(e[Yn]=[]),e[Yn].push(n)}function p_(e,n){if(e[Yn]===null)return;let t=e[Yn].indexOf(n);t!==-1&&e[Yn].splice(t,1)}function Ii(e){let n=e[Ke];return un(n)?n[Ke]:n}function $h(e){return e[so]??=[]}function zh(e){return e.cleanup??=[]}function m_(e,n,t,i){let r=$h(n);r.push(t),e.firstCreatePass&&zh(e).push(i,r.length-1)}var ie={lFrame:x_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var uh=!1;function g_(){return ie.lFrame.elementDepthCount}function v_(){ie.lFrame.elementDepthCount++}function Gh(){ie.lFrame.elementDepthCount--}function Wh(){return ie.bindingsEnabled}function qh(){return ie.skipHydrationRootTNode!==null}function Yh(e){return ie.skipHydrationRootTNode===e}function Zh(){ie.skipHydrationRootTNode=null}function z(){return ie.lFrame.lView}function Le(){return ie.lFrame.tView}function zt(e){return ie.lFrame.contextLView=e,e[$e]}function Gt(e){return ie.lFrame.contextLView=null,e}function tt(){let e=Kh();for(;e!==null&&e.type===64;)e=e.parent;return e}function Kh(){return ie.lFrame.currentTNode}function y_(){let e=ie.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function lo(e,n){let t=ie.lFrame;t.currentTNode=e,t.isParent=n}function Qh(){return ie.lFrame.isParent}function Xh(){ie.lFrame.isParent=!1}function __(){return ie.lFrame.contextLView}function Jh(){return uh}function Ds(e){let n=uh;return uh=e,n}function wl(){let e=ie.lFrame,n=e.bindingRootIndex;return n===-1&&(n=e.bindingRootIndex=e.tView.bindingStartIndex),n}function b_(e){return ie.lFrame.bindingIndex=e}function Ri(){return ie.lFrame.bindingIndex++}function ep(e){let n=ie.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function w_(){return ie.lFrame.inI18n}function E_(e,n){let t=ie.lFrame;t.bindingIndex=t.bindingRootIndex=e,El(n)}function C_(){return ie.lFrame.currentDirectiveIndex}function El(e){ie.lFrame.currentDirectiveIndex=e}function D_(e){let n=ie.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function Cl(){return ie.lFrame.currentQueryIndex}function Os(e){ie.lFrame.currentQueryIndex=e}function kS(e){let n=e[V];return n.type===2?n.declTNode:n.type===1?e[Dt]:null}function tp(e,n,t){if(t&4){let r=n,o=e;for(;r=r.parent,r===null&&!(t&1);)if(r=kS(o),r===null||(o=o[fr],r.type&10))break;if(r===null)return!1;n=r,e=o}let i=ie.lFrame=I_();return i.currentTNode=n,i.lView=e,!0}function Dl(e){let n=I_(),t=e[V];ie.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function I_(){let e=ie.lFrame,n=e===null?null:e.child;return n===null?x_(e):n}function x_(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function S_(){let e=ie.lFrame;return ie.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var np=S_;function Il(){let e=S_();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function M_(e){return(ie.lFrame.contextLView=h_(e,ie.lFrame.contextLView))[$e]}function ei(){return ie.lFrame.selectedIndex}function ki(e){ie.lFrame.selectedIndex=e}function Fs(){let e=ie.lFrame;return vl(e.tView,e.selectedIndex)}function Ps(){ie.lFrame.currentNamespace=Lh}function ip(){return ie.lFrame.currentNamespace}var T_=!0;function xl(){return T_}function Sl(e){T_=e}function fh(e,n=null,t=null,i){let r=rp(e,n,t,i);return r.resolveInjectorInitializers(),r}function rp(e,n=null,t=null,i,r=new Set){let o=[t||gt,r_(e)],s;return new lr(o,n||oo(),s||null,r)}var re=class e{static THROW_IF_NOT_FOUND=sr;static NULL=new Cs;static create(n,t){if(Array.isArray(n))return fh({name:""},t,n,"");{let i=n.name??"";return fh({name:i},n.parent,n.providers,i)}}static \u0275prov=E({token:e,providedIn:"any",factory:()=>I(Mh)});static __NG_ELEMENT_ID__=-1},W=new y(""),dt=(()=>{class e{static __NG_ELEMENT_ID__=NS;static __NG_ENV_ID__=t=>t}return e})(),rl=class extends dt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return gr(this._lView)}onDestroy(n){let t=this._lView;return Hh(t,n),()=>p_(t,n)}};function NS(){return new rl(z())}var A_=!1,R_=new y(""),ti=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Pe(!1);debugTaskTracker=f(R_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new H(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=E({token:e,providedIn:"root",factory:()=>new e})}return e})(),hh=class extends M{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,c_()&&(this.destroyRef=f(dt,{optional:!0})??void 0,this.pendingTasks=f(ti,{optional:!0})??void 0)}emit(n){let t=O(null);try{super.next(n)}finally{O(t)}}subscribe(n,t,i){let r=n,o=t||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof Ie&&n.add(a),a}wrapInTimeout(n){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},oe=hh;function ol(...e){}function op(e){let n,t;function i(){e=ol;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch(r){}}return n=setTimeout(()=>{e(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),i()})),()=>i()}function k_(e){return queueMicrotask(()=>e()),()=>{e=ol}}var sp="isAngularZone",Is=sp+"_ID",OS=0,j=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new oe(!1);onMicrotaskEmpty=new oe(!1);onStable=new oe(!1);onError=new oe(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=A_}=n;if(typeof Zone>"u")throw new x(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,LS(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(sp)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new x(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new x(909,!1)}run(n,t,i){return this._inner.run(n,t,i)}runTask(n,t,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,FS,ol,ol);try{return o.runTask(s,t,i)}finally{o.cancelTask(s)}}runGuarded(n,t,i){return this._inner.runGuarded(n,t,i)}runOutsideAngular(n){return this._outer.run(n)}},FS={};function ap(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function PS(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){op(()=>{e.callbackScheduled=!1,ph(e),e.isCheckStableRunning=!0,ap(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),ph(e)}function LS(e){let n=()=>{PS(e)},t=OS++;e._inner=e._inner.fork({name:"angular",properties:{[sp]:!0,[Is]:t,[Is+t]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(jS(c))return i.invokeTask(o,s,a,c);try{return Wy(e),i.invokeTask(o,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),qy(e)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return Wy(e),i.invoke(o,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!VS(c)&&n(),qy(e)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,ph(e),ap(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function ph(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function Wy(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function qy(e){e._nesting--,ap(e)}var xs=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new oe;onMicrotaskEmpty=new oe;onStable=new oe;onError=new oe;run(n,t,i){return n.apply(t,i)}runGuarded(n,t,i){return n.apply(t,i)}runOutsideAngular(n){return n()}runTask(n,t,i,r){return n.apply(t,i)}};function jS(e){return N_(e,"__ignore_ng_zone__")}function VS(e){return N_(e,"__scheduler_tick__")}function N_(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var Ye=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Wt=new y("",{factory:()=>{let e=f(j),n=f(Ee),t;return i=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw i}):(t??=n.get(Ye),t.handleError(i))})}}}),O_={provide:Si,useValue:()=>{let e=f(Ye,{optional:!0})},multi:!0};function Te(e,n){let[t,i,r]=Ff(e,n?.equal),o=t,s=o[qe];return o.set=i,o.update=r,o.asReadonly=Ml.bind(o),o}function Ml(){let e=this[qe];if(e.readonlyFn===void 0){let n=()=>this();n[qe]=e,e.readonlyFn=n}return e.readonlyFn}var uo=(()=>{class e{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=BS}return e})();function BS(){return new uo(z(),tt())}var Sn=class{},Ls=new y("",{factory:()=>!0});var cp=new y(""),fo=(()=>{class e{internalPendingTasks=f(ti);scheduler=f(Sn);errorHandler=f(Wt);add(){let t=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(t)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(t))}}run(t){let i=this.add();t().catch(this.errorHandler).finally(i)}static \u0275prov=E({token:e,providedIn:"root",factory:()=>new e})}return e})(),Tl=(()=>{class e{static \u0275prov=E({token:e,providedIn:"root",factory:()=>new mh})}return e})(),mh=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,i=this.queues.get(t);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,i]of this.queues)t===null?n||=this.flushQueue(i):n||=t.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},sl=class{[qe];constructor(n){this[qe]=n}destroy(){this[qe].destroy()}};function hn(e,n){let t=n?.injector??f(re),i=n?.manualCleanup!==!0?t.get(dt):null,r,o=t.get(uo,null,{optional:!0}),s=t.get(Sn);return o!==null?(r=$S(o.view,s,e),i instanceof rl&&i._lView===o.view&&(i=null)):r=zS(e,t.get(Tl),s),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new sl(r)}var F_=A(_({},Lf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=Ds(!1);try{jf(this)}finally{Ds(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=O(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],O(e)}}}),US=A(_({},F_),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(vi(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),HS=A(_({},F_),{consumerMarkedDirty(){this.view[$]|=8192,vr(this.view),this.notifier.notify(13)},destroy(){if(vi(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[Di]?.delete(this)}});function $S(e,n,t){let i=Object.create(HS);return i.view=e,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=P_(i,t),e[Di]??=new Set,e[Di].add(i),i.consumerMarkedDirty(i),i}function zS(e,n,t){let i=Object.create(US);return i.fn=P_(i,e),i.scheduler=n,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function P_(e,n){return()=>{n(t=>(e.cleanupFns??=[]).push(t))}}function _o(e){return{toString:e}.toString()}var Al="__parameters__";function XS(e){return function(...t){if(e){let i=e(...t);for(let r in i)this[r]=i[r]}}}function JS(e,n,t){return _o(()=>{let i=XS(n);function r(...o){if(this instanceof r)return i.apply(this,o),this;let s=new r(...o);return a.annotation=s,a;function a(c,l,d){let u=c.hasOwnProperty(Al)?c[Al]:Object.defineProperty(c,Al,{value:[]})[Al];for(;u.length<=d;)u.push(null);return(u[d]=u[d]||[]).push(s),c}}return r.prototype.ngMetadataName=e,r.annotationCls=r,r})}var im=Jy(JS("Inject",e=>({token:e})),-1);function eM(e){return typeof e=="function"}function gb(e,n,t,i){n!==null?n.applyValueToInputSignal(n,i):e[t]=i}var jl=class{previousValue;currentValue;firstChange;constructor(n,t,i){this.previousValue=n,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},xt=(()=>{let e=()=>vb;return e.ngInherit=!0,e})();function vb(e){return e.type.prototype.ngOnChanges&&(e.setInput=nM),tM}function tM(){let e=_b(this),n=e?.current;if(n){let t=e.previous;if(t===xi)e.previous=n;else for(let i in n)t[i]=n[i];e.current=null,this.ngOnChanges(n)}}function nM(e,n,t,i,r){let o=this.declaredInputs[i],s=_b(e)||iM(e,{previous:xi,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new jl(l&&l.currentValue,t,c===xi),gb(e,n,r,t)}var yb="__ngSimpleChanges__";function _b(e){return e[yb]||null}function iM(e,n){return e[yb]=n}var L_=[];var ve=function(e,n=null,t){for(let i=0;i<L_.length;i++){let r=L_[i];r(e,n,t)}},de=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(de||{});function rM(e,n,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=vb(n);(t.preOrderHooks??=[]).push(e,s),(t.preOrderCheckHooks??=[]).push(e,s)}r&&(t.preOrderHooks??=[]).push(0-e,r),o&&((t.preOrderHooks??=[]).push(e,o),(t.preOrderCheckHooks??=[]).push(e,o))}function bb(e,n){for(let t=n.directiveStart,i=n.directiveEnd;t<i;t++){let o=e.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(e.contentHooks??=[]).push(-t,s),a&&((e.contentHooks??=[]).push(t,a),(e.contentCheckHooks??=[]).push(t,a)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function Ol(e,n,t){wb(e,n,3,t)}function Fl(e,n,t,i){(e[$]&3)===t&&wb(e,n,t,i)}function lp(e,n){let t=e[$];(t&3)===n&&(t&=16383,t+=1,e[$]=t)}function wb(e,n,t,i){let r=i!==void 0?e[hr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(e[hr]+=65536),(a<o||o==-1)&&(oM(e,t,n,c),e[hr]=(e[hr]&4294901760)+c+2),c++}function j_(e,n){ve(de.LifecycleHookStart,e,n);let t=O(null);try{n.call(e)}finally{O(t),ve(de.LifecycleHookEnd,e,n)}}function oM(e,n,t,i){let r=t[i]<0,o=t[i+1],s=r?-t[i]:t[i],a=e[s];r?e[$]>>14<e[hr]>>16&&(e[$]&3)===n&&(e[$]+=16384,j_(a,o)):j_(a,o)}var po=-1,_r=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function sM(e){return(e.flags&8)!==0}function aM(e){return(e.flags&16)!==0}function cM(e,n,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let o=t[i++],s=t[i++],a=t[i++];e.setAttribute(n,s,a,o)}else{let o=r,s=t[++i];lM(o)?e.setProperty(n,o,s):e.setAttribute(n,o,s),i++}}return i}function Eb(e){return e===3||e===4||e===6}function lM(e){return e.charCodeAt(0)===64}function mo(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?V_(e,t,r,null,n[++i]):V_(e,t,r,null,null))}}return e}function V_(e,n,t,i,r){let o=0,s=e.length;if(n===-1)s=-1;else for(;o<e.length;){let a=e[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<e.length;){let a=e[o];if(typeof a=="number")break;if(a===t){r!==null&&(e[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(e.splice(s,0,n),o=s+1),e.splice(o++,0,t),r!==null&&e.splice(o++,0,r)}function Cb(e){return e!==po}function Vl(e){return e&32767}function dM(e){return e>>16}function Bl(e,n){let t=dM(e),i=n;for(;t>0;)i=i[fr],t--;return i}var wp=!0;function Ul(e){let n=wp;return wp=e,n}var uM=256,Db=uM-1,Ib=5,fM=0,kn={};function hM(e,n,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(dr)&&(i=t[dr]),i==null&&(i=t[dr]=fM++);let r=i&Db,o=1<<r;n.data[e+(r>>Ib)]|=o}function Hl(e,n){let t=xb(e,n);if(t!==-1)return t;let i=n[V];i.firstCreatePass&&(e.injectorIndex=n.length,dp(i.data,e),dp(n,null),dp(i.blueprint,null));let r=rm(e,n),o=e.injectorIndex;if(Cb(r)){let s=Vl(r),a=Bl(r,n),c=a[V].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function dp(e,n){e.push(0,0,0,0,0,0,0,0,n)}function xb(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function rm(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,i=null,r=n;for(;r!==null;){if(i=Rb(r),i===null)return po;if(t++,r=r[fr],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return po}function Ep(e,n,t){hM(e,n,t)}function pM(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let i=t.length,r=0;for(;r<i;){let o=t[r];if(Eb(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof t[r]=="string";)r++;else{if(o===n)return t[r+1];r=r+2}}}return null}function Sb(e,n,t){if(t&8||e!==void 0)return e;fl(n,"NodeInjector")}function Mb(e,n,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=e[Kn],o=Ct(void 0);try{return r?r.get(n,i,t&8):xh(n,i,t&8)}finally{Ct(o)}}return Sb(i,n,t)}function Tb(e,n,t,i=0,r){if(e!==null){if(n[$]&2048&&!(i&2)){let s=yM(e,n,t,i,kn);if(s!==kn)return s}let o=Ab(e,n,t,i,kn);if(o!==kn)return o}return Mb(n,t,i,r)}function Ab(e,n,t,i,r){let o=gM(t);if(typeof o=="function"){if(!tp(n,e,i))return i&1?Sb(r,t,i):Mb(n,t,i,r);try{let s;if(s=o(i),s==null&&!(i&8))fl(t);else return s}finally{np()}}else if(typeof o=="number"){let s=null,a=xb(e,n),c=po,l=i&1?n[It][Dt]:null;for((a===-1||i&4)&&(c=a===-1?rm(e,n):n[a+8],c===po||!U_(i,!1)?a=-1:(s=n[V],a=Vl(c),n=Bl(c,n)));a!==-1;){let d=n[V];if(B_(o,a,d.data)){let u=mM(a,n,t,s,i,l);if(u!==kn)return u}c=n[a+8],c!==po&&U_(i,n[V].data[a+8]===l)&&B_(o,a,n)?(s=d,a=Vl(c),n=Bl(c,n)):a=-1}}return r}function mM(e,n,t,i,r,o){let s=n[V],a=s.data[e+8],c=i==null?Jn(a)&&wp:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Pl(a,s,t,c,l);return d!==null?Hs(n,s,d,a,r):kn}function Pl(e,n,t,i,r){let o=e.providerIndexes,s=n.data,a=o&1048575,c=e.directiveStart,l=e.directiveEnd,d=o>>20,u=i?a:a+d,h=r?a+d:l;for(let p=u;p<h;p++){let m=s[p];if(p<c&&t===m||p>=c&&m.type===t)return p}if(r){let p=s[c];if(p&&Rn(p)&&p.type===t)return c}return null}function Hs(e,n,t,i,r){let o=e[t],s=n.data;if(o instanceof _r){let a=o;if(a.resolving)throw Ih("");let c=Ul(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],d,u=a.injectImpl?Ct(a.injectImpl):null,h=tp(e,i,0);try{o=e[t]=a.factory(void 0,r,s,e,i),n.firstCreatePass&&t>=i.directiveStart&&rM(t,s[t],n)}finally{u!==null&&Ct(u),Ul(c),a.resolving=!1,np()}}return o}function gM(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=e.hasOwnProperty(dr)?e[dr]:void 0;return typeof n=="number"?n>=0?n&Db:vM:n}function B_(e,n,t){let i=1<<e;return!!(t[n+(e>>Ib)]&i)}function U_(e,n){return!(e&2)&&!(e&1&&n)}var yr=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,i){return Tb(this._tNode,this._lView,n,ar(i),t)}};function vM(){return new yr(tt(),z())}function ft(e){return _o(()=>{let n=e.prototype.constructor,t=n[Es]||Cp(n),i=Object.prototype,r=Object.getPrototypeOf(e.prototype).constructor;for(;r&&r!==i;){let o=r[Es]||Cp(r);if(o&&o!==t)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Cp(e){return gh(e)?()=>{let n=Cp(lt(e));return n&&n()}:Ci(e)}function yM(e,n,t,i,r){let o=e,s=n;for(;o!==null&&s!==null&&s[$]&2048&&!co(s);){let a=Ab(o,s,t,i|2,kn);if(a!==kn)return a;let c=o.parent;if(!c){let l=s[Oh];if(l){let d=l.get(t,kn,i&-5);if(d!==kn)return d}c=Rb(s),s=s[fr]}o=c}return r}function Rb(e){let n=e[V],t=n.type;return t===2?n.declTNode:t===1?e[Dt]:null}function om(e){return pM(tt(),e)}function _M(){return bo(tt(),z())}function bo(e,n){return new Q(fn(e,n))}var Q=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=_M}return e})();function kb(e){return e instanceof Q?e.nativeElement:e}function bM(){return this._results[Symbol.iterator]()}var ni=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new M}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let i=t_(n);(this._changesDetected=!e_(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=bM};function Nb(e){return(e.flags&128)===128}var sm=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(sm||{}),Ob=new Map,wM=0;function EM(){return wM++}function CM(e){Ob.set(e[Qn],e)}function Dp(e){Ob.delete(e[Qn])}var H_="__ngContext__";function go(e,n){Xn(n)?(e[H_]=n[Qn],CM(n)):e[H_]=n}function Fb(e){return Lb(e[ao])}function Pb(e){return Lb(e[Bt])}function Lb(e){for(;e!==null&&!un(e);)e=e[Bt];return e}var DM;function am(e){DM=e}var wo=new y("",{factory:()=>IM}),IM="ng";var td=new y(""),Cr=new y("",{providedIn:"platform",factory:()=>"unknown"}),Zs=new y(""),Dr=new y("",{factory:()=>f(W).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var jb="r";var Vb="di";var Bb=!1,Ub=new y("",{factory:()=>Bb});var $_=new WeakMap;function xM(e,n){if(e==null||typeof e!="object")return;let t=$_.get(e);t||(t=new WeakSet,$_.set(e,t)),t.add(n)}var SM=(e,n,t,i)=>{};function MM(e,n,t,i){SM(e,n,t,i)}function nd(e){return(e.flags&32)===32}var TM=()=>null;function Hb(e,n,t=!1){return TM(e,n,t)}function $b(e,n){let t=e.contentQueries;if(t!==null){let i=O(null);try{for(let r=0;r<t.length;r+=2){let o=t[r],s=t[r+1];if(s!==-1){let a=e.data[s];Os(o),a.contentQueries(2,n[s],s)}}}finally{O(i)}}}function Ip(e,n,t){Os(0);let i=O(null);try{n(e,t)}finally{O(i)}}function zb(e,n,t){if(Ph(n)){let i=O(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=e.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{O(i)}}}var gn=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(gn||{});var Rl;function AM(){if(Rl===void 0&&(Rl=null,Tn.trustedTypes))try{Rl=Tn.trustedTypes.createPolicy("angular",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch(e){}return Rl}function id(e){return AM()?.createHTML(e)||e}var ii=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${al})`}},xp=class extends ii{getTypeName(){return"HTML"}},Sp=class extends ii{getTypeName(){return"Style"}},Mp=class extends ii{getTypeName(){return"Script"}},Tp=class extends ii{getTypeName(){return"URL"}},Ap=class extends ii{getTypeName(){return"ResourceURL"}};function Pn(e){return e instanceof ii?e.changingThisBreaksApplicationSecurity:e}function Oi(e,n){let t=Gb(e);if(t!=null&&t!==n){if(t==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${t} (see ${al})`)}return t===n}function Gb(e){return e instanceof ii&&e.getTypeName()||null}function cm(e){return new xp(e)}function lm(e){return new Sp(e)}function dm(e){return new Mp(e)}function um(e){return new Tp(e)}function fm(e){return new Ap(e)}function RM(e){let n=new kp(e);return kM()?new Rp(n):n}var Rp=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let t=new window.DOMParser().parseFromString(id(n),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(n):(t.firstChild?.remove(),t)}catch(t){return null}}},kp=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let t=this.inertDocument.createElement("template");return t.innerHTML=id(n),t}};function kM(){try{return!!new window.DOMParser().parseFromString(id(""),"text/html")}catch(e){return!1}}var NM=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Ks(e){return e=String(e),e.match(NM)?e:"unsafe:"+e}function ri(e){let n={};for(let t of e.split(","))n[t]=!0;return n}function Qs(...e){let n={};for(let t of e)for(let i in t)t.hasOwnProperty(i)&&(n[i]=!0);return n}var Wb=ri("area,br,col,hr,img,wbr"),qb=ri("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Yb=ri("rp,rt"),OM=Qs(Yb,qb),FM=Qs(qb,ri("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),PM=Qs(Yb,ri("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),z_=Qs(Wb,FM,PM,OM),Zb=ri("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),LM=ri("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),jM=ri("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),VM=Qs(Zb,LM,jM),BM=ri("script,style,template");var Np=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let t=n.firstChild,i=!0,r=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?i=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,i&&t.firstChild){r.push(t),t=$M(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let o=HM(t);if(o){t=o;break}t=r.pop()}}return this.buf.join("")}startElement(n){let t=G_(n).toLowerCase();if(!z_.hasOwnProperty(t))return this.sanitizedSomething=!0,!BM.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!VM.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;Zb[a]&&(c=Ks(c)),this.buf.push(" ",s,'="',W_(c),'"')}return this.buf.push(">"),!0}endElement(n){let t=G_(n).toLowerCase();z_.hasOwnProperty(t)&&!Wb.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(n){this.buf.push(W_(n))}};function UM(e,n){return(e.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function HM(e){let n=e.nextSibling;if(n&&e!==n.previousSibling)throw Kb(n);return n}function $M(e){let n=e.firstChild;if(n&&UM(e,n))throw Kb(n);return n}function G_(e){let n=e.nodeName;return typeof n=="string"?n:"FORM"}function Kb(e){return new Error(`Failed to sanitize html because the element is clobbered: ${e.outerHTML}`)}var zM=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,GM=/([^\#-~ |!])/g;function W_(e){return e.replace(/&/g,"&amp;").replace(zM,function(n){let t=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((t-55296)*1024+(i-56320)+65536)+";"}).replace(GM,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var kl;function hm(e,n){let t=null;try{kl=kl||RM(e);let i=n?String(n):"";t=kl.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=t.innerHTML,t=kl.getInertBodyElement(i)}while(i!==o);let a=new Np().sanitizeChildren(q_(t)||t);return id(a)}finally{if(t){let i=q_(t)||t;for(;i.firstChild;)i.firstChild.remove()}}}function q_(e){return"content"in e&&WM(e)?e.content:null}function WM(e){return e.nodeType===Node.ELEMENT_NODE&&e.nodeName==="TEMPLATE"}function qM(e,n){return e.createText(n)}function YM(e,n,t){e.setValue(n,t)}function Qb(e,n,t){return e.createElement(n,t)}function $l(e,n,t,i,r){e.insertBefore(n,t,i,r)}function Xb(e,n,t){e.appendChild(n,t)}function Y_(e,n,t,i,r){i!==null?$l(e,n,t,i,r):Xb(e,n,t)}function Jb(e,n,t,i){e.removeChild(null,n,t,i)}function ZM(e,n,t){e.setAttribute(n,"style",t)}function KM(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function e0(e,n,t){let{mergedAttrs:i,classes:r,styles:o}=t;i!==null&&cM(e,n,i),r!==null&&KM(e,n,r),o!==null&&ZM(e,n,o)}var nt=(function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e})(nt||{});function vt(e){let n=QM();return n?n.sanitize(nt.URL,e)||"":Oi(e,"URL")?Pn(e):Ks(ul(e))}function QM(){let e=z();return e&&e[dn].sanitizer}function t0(e){return e instanceof Function?e():e}function XM(e,n,t){let i=e.length;for(;;){let r=e.indexOf(n,t);if(r===-1)return r;if(r===0||e.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||e.charCodeAt(r+o)<=32)return r}t=r+1}}var n0="ng-template";function JM(e,n,t,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&XM(n[r+1].toLowerCase(),t,0)!==-1)return!0}else if(pm(e))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===t)return!0}return!1}function pm(e){return e.type===4&&e.value!==n0}function eT(e,n,t){let i=e.type===4&&!t?n0:e.value;return n===i}function tT(e,n,t){let i=4,r=e.attrs,o=r!==null?rT(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!pn(i)&&!pn(c))return!1;if(s&&pn(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!eT(e,c,t)||c===""&&n.length===1){if(pn(i))return!1;s=!0}}else if(i&8){if(r===null||!JM(e,r,c,t)){if(pn(i))return!1;s=!0}}else{let l=n[++a],d=nT(c,r,pm(e),t);if(d===-1){if(pn(i))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=r[d+1].toLowerCase(),i&2&&l!==u){if(pn(i))return!1;s=!0}}}}return pn(i)||s}function pn(e){return(e&1)===0}function nT(e,n,t,i){if(n===null)return-1;let r=0;if(i||!t){let o=!1;for(;r<n.length;){let s=n[r];if(s===e)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return oT(n,e)}function i0(e,n,t=!1){for(let i=0;i<n.length;i++)if(tT(e,n[i],t))return!0;return!1}function iT(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function rT(e){for(let n=0;n<e.length;n++){let t=e[n];if(Eb(t))return n}return e.length}function oT(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let i=e[t];if(typeof i=="number")return-1;if(i===n)return t;t++}return-1}function sT(e,n){e:for(let t=0;t<n.length;t++){let i=n[t];if(e.length===i.length){for(let r=0;r<e.length;r++)if(e[r]!==i[r])continue e;return!0}}return!1}function Z_(e,n){return e?":not("+n.trim()+")":n}function aT(e){let n=e[0],t=1,i=2,r="",o=!1;for(;t<e.length;){let s=e[t];if(typeof s=="string")if(i&2){let a=e[++t];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!pn(s)&&(n+=Z_(o,r),r=""),i=s,o=o||!pn(i);t++}return r!==""&&(n+=Z_(o,r)),n}function cT(e){return e.map(aT).join(",")}function lT(e){let n=[],t=[],i=1,r=2;for(;i<e.length;){let o=e[i];if(typeof o=="string")r===2?o!==""&&n.push(o,e[++i]):r===8&&t.push(o);else{if(!pn(r))break;r=o}i++}return t.length&&n.push(1,...t),n}var Yt={};function mm(e,n,t,i,r,o,s,a,c,l,d){let u=Oe+i,h=u+r,p=dT(u,h),m=typeof l=="function"?l():l;return p[V]={type:e,blueprint:p,template:t,queries:null,viewQuery:a,declTNode:n,data:p.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:m,incompleteFirstPass:!1,ssrId:d}}function dT(e,n){let t=[];for(let i=0;i<n;i++)t.push(i<e?null:Yt);return t}function uT(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=mm(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function gm(e,n,t,i,r,o,s,a,c,l,d){let u=n.blueprint.slice();return u[ln]=r,u[$]=i|4|128|8|64|1024,(l!==null||e&&e[$]&2048)&&(u[$]|=2048),Bh(u),u[Ke]=u[fr]=e,u[$e]=t,u[dn]=s||e&&e[dn],u[Me]=a||e&&e[Me],u[Kn]=c||e&&e[Kn]||null,u[Dt]=o,u[Qn]=EM(),u[ur]=d,u[Oh]=l,u[It]=n.type==2?e[It]:u,u}function fT(e,n,t){let i=fn(n,e),r=uT(t),o=e[dn].rendererFactory,s=vm(e,gm(e,r,null,r0(t),i,n,null,o.createRenderer(i,t),null,null,null));return e[n.index]=s}function r0(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function o0(e,n,t,i){if(t===0)return-1;let r=n.length;for(let o=0;o<t;o++)n.push(i),e.blueprint.push(i),e.data.push(null);return r}function vm(e,n){return e[ao]?e[Nh][Bt]=n:e[ao]=n,e[Nh]=n,n}function w(e=1){s0(Le(),z(),ei()+e,!1)}function s0(e,n,t,i){if(!i)if((n[$]&3)===3){let o=e.preOrderCheckHooks;o!==null&&Ol(n,o,t)}else{let o=e.preOrderHooks;o!==null&&Fl(n,o,0,t)}ki(t)}var rd=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(rd||{});function Op(e,n,t,i){let r=O(null);try{let[o,s,a]=e.inputs[t],c=null;(s&rd.SignalBased)!==0&&(c=n[o][qe]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),e.setInput!==null?e.setInput(n,c,i,t,o):gb(n,c,o,i)}finally{O(r)}}var Nn=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(Nn||{}),hT;function ym(e,n){return hT(e,n)}var pG=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Fp=new WeakMap,Vs=new WeakSet;function pT(e,n){let t=Fp.get(e);if(!t||t.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=t.length-1;o>=0;o--){let s=t[o],a=s.parentNode;s===n?(t.splice(o,1),Vs.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(t.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function mT(e,n){let t=Fp.get(e);t?t.includes(n)||t.push(n):Fp.set(e,[n])}var br=new Set,od=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(od||{}),vn=new y(""),K_=new Set;function Fi(e){K_.has(e)||(K_.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var sd=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=E({token:e,providedIn:"root",factory:()=>new e})}return e})(),_m=[0,1,2,3],bm=(()=>{class e{ngZone=f(j);scheduler=f(Sn);errorHandler=f(Ye,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){f(vn,{optional:!0})}execute(){let t=this.sequences.size>0;t&&ve(de.AfterRenderHooksStart),this.executing=!0;for(let i of _m)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&ve(de.AfterRenderHooksEnd)}register(t){let{view:i}=t;i!==void 0?((i[pr]??=[]).push(t),vr(i),i[$]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,i){return i?i.run(od.AFTER_NEXT_RENDER,t):t()}static \u0275prov=E({token:e,providedIn:"root",factory:()=>new e})}return e})(),$s=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,i,r,o,s=null){this.impl=n,this.hooks=t,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[pr];n&&(this.view[pr]=n.filter(t=>t!==this))}};function Zt(e,n){let t=n?.injector??f(re);return Fi("NgAfterNextRender"),vT(e,t,n,!0)}function gT(e){return e instanceof Function?[void 0,void 0,e,void 0]:[e.earlyRead,e.write,e.mixedReadWrite,e.read]}function vT(e,n,t,i){let r=n.get(sd);r.impl??=n.get(bm);let o=n.get(vn,null,{optional:!0}),s=t?.manualCleanup!==!0?n.get(dt):null,a=n.get(uo,null,{optional:!0}),c=new $s(r.impl,gT(e),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var a0=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:f(Ee)})});function c0(e,n,t){let i=e.get(a0);if(Array.isArray(n))for(let r of n)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),t?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(e)}function yT(e,n){let t=e.get(a0);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)t.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function _T(e,n){for(let[t,i]of n)c0(e,i.animateFns)}function Q_(e,n,t,i){let r=e?.[Ti]?.enter;n!==null&&r&&r.has(t.index)&&_T(i,r)}function ho(e,n,t,i,r,o,s,a){if(r!=null){let c,l=!1;un(r)?c=r:Xn(r)&&(l=!0,r=r[ln]);let d=Ut(r);e===0&&i!==null?(Q_(a,i,o,t),s==null?Xb(n,i,d):$l(n,i,d,s||null,!0)):e===1&&i!==null?(Q_(a,i,o,t),$l(n,i,d,s||null,!0),pT(o,d)):e===2?(a?.[Ti]?.leave?.has(o.index)&&mT(o,d),Vs.delete(d),X_(a,o,t,u=>{if(Vs.has(d)){Vs.delete(d);return}Jb(n,d,l,u)})):e===3&&(Vs.delete(d),X_(a,o,t,()=>{n.destroyNode(d)})),c!=null&&AT(n,e,t,c,o,i,s)}}function bT(e,n){l0(e,n),n[ln]=null,n[Dt]=null}function wT(e,n,t,i,r,o){i[ln]=r,i[Dt]=n,cd(e,i,t,1,r,o)}function l0(e,n){n[dn].changeDetectionScheduler?.notify(9),cd(e,n,n[Me],2,null,null)}function ET(e){let n=e[ao];if(!n)return up(e[V],e);for(;n;){let t=null;if(Xn(n))t=n[ao];else{let i=n[Ve];i&&(t=i)}if(!t){for(;n&&!n[Bt]&&n!==e;)Xn(n)&&up(n[V],n),n=n[Ke];n===null&&(n=e),Xn(n)&&up(n[V],n),t=n&&n[Bt]}n=t}}function wm(e,n){let t=e[mr],i=t.indexOf(n);t.splice(i,1)}function ad(e,n){if(gr(n))return;let t=n[Me];t.destroyNode&&cd(e,n,t,3,null,null),ET(n)}function up(e,n){if(gr(n))return;let t=O(null);try{n[$]&=-129,n[$]|=256,n[Nt]&&vi(n[Nt]),IT(e,n),DT(e,n),n[V].type===1&&n[Me].destroy();let i=n[Mi];if(i!==null&&un(n[Ke])){i!==n[Ke]&&wm(i,n);let r=n[An];r!==null&&r.detachView(e)}Dp(n)}finally{O(t)}}function X_(e,n,t,i){let r=e?.[Ti];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);e&&br.add(e[Qn]),c0(t,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),CT(e,i)}else e&&br.delete(e[Qn]),i(!1)},r)}function CT(e,n){let t=e[Ti]?.running;if(t){t.then(()=>{e[Ti].running=void 0,br.delete(e[Qn]),n(!0)});return}n(!1)}function DT(e,n){let t=e.cleanup,i=n[so];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[t[s+1]];t[s].call(a)}i!==null&&(n[so]=null);let r=n[Yn];if(r!==null){n[Yn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Di];if(o!==null){n[Di]=null;for(let s of o)s.destroy()}}function IT(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=n[t[i]];if(!(r instanceof _r)){let o=t[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];ve(de.LifecycleHookStart,a,c);try{c.call(a)}finally{ve(de.LifecycleHookEnd,a,c)}}else{ve(de.LifecycleHookStart,r,o);try{o.call(r)}finally{ve(de.LifecycleHookEnd,r,o)}}}}}function d0(e,n,t){return xT(e,n.parent,t)}function xT(e,n,t){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return t[ln];if(Jn(i)){let{encapsulation:r}=e.data[i.directiveStart+i.componentOffset];if(r===gn.None||r===gn.Emulated)return null}return fn(i,t)}function u0(e,n,t){return MT(e,n,t)}function ST(e,n,t){return e.type&40?fn(e,t):null}var MT=ST,J_;function Em(e,n,t,i){let r=d0(e,i,n),o=n[Me],s=i.parent||n[Dt],a=u0(s,i,n);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Y_(o,r,t[c],a,!1);else Y_(o,r,t,a,!1);J_!==void 0&&J_(o,i,n,t,r)}function Bs(e,n){if(n!==null){let t=n.type;if(t&3)return fn(n,e);if(t&4)return Pp(-1,e[n.index]);if(t&8){let i=n.child;if(i!==null)return Bs(e,i);{let r=e[n.index];return un(r)?Pp(-1,r):Ut(r)}}else{if(t&128)return Bs(e,n.next);if(t&32)return ym(n,e)()||Ut(e[n.index]);{let i=f0(e,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Ii(e[It]);return Bs(r,i)}else return Bs(e,n.next)}}}return null}function f0(e,n){if(n!==null){let i=e[It][Dt],r=n.projection;return i.projection[r]}return null}function Pp(e,n){let t=Ve+e+1;if(t<n.length){let i=n[t],r=i[V].firstChild;if(r!==null)return Bs(i,r)}return n[Ai]}function Cm(e,n,t,i,r,o,s){for(;t!=null;){let a=i[Kn];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(s&&n===0&&(c&&go(Ut(c),i),t.flags|=2),!nd(t))if(l&8)Cm(e,n,t.child,i,r,o,!1),ho(n,e,a,r,c,t,o,i);else if(l&32){let d=ym(t,i),u;for(;u=d();)ho(n,e,a,r,u,t,o,i);ho(n,e,a,r,c,t,o,i)}else l&16?h0(e,n,i,t,r,o):ho(n,e,a,r,c,t,o,i);t=s?t.projectionNext:t.next}}function cd(e,n,t,i,r,o){Cm(t,i,e.firstChild,n,r,o,!1)}function TT(e,n,t){let i=n[Me],r=d0(e,t,n),o=t.parent||n[Dt],s=u0(o,t,n);h0(i,0,n,t,r,s)}function h0(e,n,t,i,r,o){let s=t[It],c=s[Dt].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];ho(n,e,t[Kn],r,d,i,o,t)}else{let l=c,d=s[Ke];Nb(i)&&(l.flags|=128),Cm(e,n,l,d,r,o,!0)}}function AT(e,n,t,i,r,o,s){let a=i[Ai],c=Ut(i);a!==c&&ho(n,e,t,o,a,r,s);for(let l=Ve;l<i.length;l++){let d=i[l];cd(d[V],d,e,n,o,a)}}function RT(e,n,t,i,r){if(n)r?e.addClass(t,i):e.removeClass(t,i);else{let o=i.indexOf("-")===-1?void 0:Nn.DashCase;r==null?e.removeStyle(t,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Nn.Important),e.setStyle(t,i,r,o))}}function p0(e,n,t,i,r){let o=ei(),s=i&2;try{ki(-1),s&&n.length>Oe&&s0(e,n,Oe,!1);let a=s?de.TemplateUpdateStart:de.TemplateCreateStart;ve(a,r,t),t(i,r)}finally{ki(o);let a=s?de.TemplateUpdateEnd:de.TemplateCreateEnd;ve(a,r,t)}}function Dm(e,n,t){LT(e,n,t),(t.flags&64)===64&&jT(e,n,t)}function ld(e,n,t=fn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?t(n,e):e[s];e[r++]=a}}}function kT(e,n,t,i){let o=i.get(Ub,Bb)||t===gn.ShadowDom||t===gn.ExperimentalIsolatedShadowDom,s=e.selectRootElement(n,o);if(s.tagName.toLowerCase()==="script")throw new x(905,!1);return NT(s),s}function NT(e){OT(e)}var OT=()=>null;function FT(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function m0(e,n,t,i,r,o){let s=n[V];if(Im(e,s,n,t,i)){Jn(e)&&PT(n,e.index);return}e.type&3&&(t=FT(t)),g0(e,n,t,i,r,o)}function g0(e,n,t,i,r,o){if(e.type&3){let s=fn(e,n);i=o!=null?o(i,e.value||"",t):i,r.setProperty(s,t,i)}else e.type&12}function PT(e,n){let t=Ht(n,e);t[$]&16||(t[$]|=64)}function LT(e,n,t){let i=t.directiveStart,r=t.directiveEnd;Jn(t)&&fT(n,t,e.data[i+t.componentOffset]),e.firstCreatePass||Hl(t,n);let o=t.initialInputs;for(let s=i;s<r;s++){let a=e.data[s],c=Hs(n,e,s,t);if(go(c,n),o!==null&&HT(n,s-i,c,a,t,o),Rn(a)){let l=Ht(t.index,n);l[$e]=Hs(n,e,s,t)}}}function jT(e,n,t){let i=t.directiveStart,r=t.directiveEnd,o=t.index,s=C_();try{ki(o);for(let a=i;a<r;a++){let c=e.data[a],l=n[a];El(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&VT(c,l)}}finally{ki(-1),El(s)}}function VT(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function v0(e,n){let t=e.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let o=t[r];i0(n,o.selectors,!1)&&(i??=[],Rn(o)?i.unshift(o):i.push(o))}return i}function BT(e,n,t,i,r,o){let s=fn(e,n);UT(n[Me],s,o,e.value,t,i,r)}function UT(e,n,t,i,r,o,s){if(o==null)e.removeAttribute(n,r,t);else{let a=s==null?ul(o):s(o,i||"",r);e.setAttribute(n,r,a,t)}}function HT(e,n,t,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Op(i,t,c,l)}}function y0(e,n,t,i,r){let o=Oe+t,s=n[V],a=r(s,n,e,i,t);n[o]=a,lo(e,!0);let c=e.type===2;return c?(e0(n[Me],a,e),(g_()===0||ks(e))&&go(a,n),v_()):go(a,n),xl()&&(!c||!nd(e))&&Em(s,n,a,e),e}function _0(e){let n=e;return Qh()?Xh():(n=n.parent,lo(n,!1)),n}function $T(e,n){let t=e[Kn];if(!t)return;let i;try{i=t.get(Wt,null)}catch(r){i=null}i?.(n)}function Im(e,n,t,i,r){let o=e.inputs?.[i],s=e.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=n.data[l];Op(u,t[l],d,r),a=!0}if(o)for(let c of o){let l=t[c],d=n.data[c];Op(d,l,i,r),a=!0}return a}function zT(e,n){let t=Ht(n,e),i=t[V];GT(i,t);let r=t[ln];r!==null&&t[ur]===null&&(t[ur]=Hb(r,t[Kn])),ve(de.ComponentStart);try{xm(i,t,t[$e])}finally{ve(de.ComponentEnd,t[$e])}}function GT(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function xm(e,n,t){Dl(n);try{let i=e.viewQuery;i!==null&&Ip(1,i,t);let r=e.template;r!==null&&p0(e,n,r,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[An]?.finishViewCreation(e),e.staticContentQueries&&$b(e,n),e.staticViewQueries&&Ip(2,e.viewQuery,t);let o=e.components;o!==null&&WT(n,o)}catch(i){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),i}finally{n[$]&=-5,Il()}}function WT(e,n){for(let t=0;t<n.length;t++)zT(e,n[t])}function Xs(e,n,t,i){let r=O(null);try{let o=n.tView,a=e[$]&4096?4096:16,c=gm(e,o,t,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=e[n.index];c[Mi]=l;let d=e[An];return d!==null&&(c[An]=d.createEmbeddedView(o)),xm(o,c,t),c}finally{O(r)}}function vo(e,n){return!n||n.firstChild===null||Nb(e)}function zs(e,n,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let o=n[t.index];o!==null&&i.push(Ut(o)),un(o)&&b0(o,i);let s=t.type;if(s&8)zs(e,n,t.child,i);else if(s&32){let a=ym(t,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=f0(n,t);if(Array.isArray(a))i.push(...a);else{let c=Ii(n[It]);zs(c[V],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function b0(e,n){for(let t=Ve;t<e.length;t++){let i=e[t],r=i[V].firstChild;r!==null&&zs(i[V],i,r,n)}e[Ai]!==e[ln]&&n.push(e[Ai])}function w0(e){if(e[pr]!==null){for(let n of e[pr])n.impl.addSequence(n);e[pr].length=0}}var E0=[];function qT(e){return e[Nt]??YT(e)}function YT(e){let n=E0.pop()??Object.create(KT);return n.lView=e,n}function ZT(e){e.lView[Nt]!==e&&(e.lView=null,E0.push(e))}var KT=A(_({},Wi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{vr(e.lView)},consumerOnSignalRead(){this.lView[Nt]=this}});function QT(e){let n=e[Nt]??Object.create(XT);return n.lView=e,n}var XT=A(_({},Wi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=Ii(e.lView);for(;n&&!C0(n[V]);)n=Ii(n);n&&Uh(n)},consumerOnSignalRead(){this.lView[Nt]=this}});function C0(e){return e.type!==2}function D0(e){if(e[Di]===null)return;let n=!0;for(;n;){let t=!1;for(let i of e[Di])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=t&&!!(e[$]&8192)}}var JT=100;function I0(e,n=0){let i=e[dn].rendererFactory,r=!1;r||i.begin?.();try{eA(e,n)}finally{r||i.end?.()}}function eA(e,n){let t=Jh();try{Ds(!0),Lp(e,n);let i=0;for(;Ns(e);){if(i===JT)throw new x(103,!1);i++,Lp(e,1)}}finally{Ds(t)}}function tA(e,n,t,i){if(gr(n))return;let r=n[$],o=!1,s=!1;Dl(n);let a=!0,c=null,l=null;o||(C0(e)?(l=qT(n),c=gi(l)):Cc()===null?(a=!1,l=QT(n),c=gi(l)):n[Nt]&&(vi(n[Nt]),n[Nt]=null));try{Bh(n),b_(e.bindingStartIndex),t!==null&&p0(e,n,t,2,i);let d=(r&3)===3;if(!o)if(d){let p=e.preOrderCheckHooks;p!==null&&Ol(n,p,null)}else{let p=e.preOrderHooks;p!==null&&Fl(n,p,0,null),lp(n,0)}if(s||nA(n),D0(n),x0(n,0),e.contentQueries!==null&&$b(e,n),!o)if(d){let p=e.contentCheckHooks;p!==null&&Ol(n,p)}else{let p=e.contentHooks;p!==null&&Fl(n,p,1),lp(n,1)}rA(e,n);let u=e.components;u!==null&&M0(n,u,0);let h=e.viewQuery;if(h!==null&&Ip(2,h,i),!o)if(d){let p=e.viewCheckHooks;p!==null&&Ol(n,p)}else{let p=e.viewHooks;p!==null&&Fl(n,p,2),lp(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[gl]){for(let p of n[gl])p();n[gl]=null}o||(w0(n),n[$]&=-73)}catch(d){throw o||vr(n),d}finally{l!==null&&(qi(l,c),a&&ZT(l)),Il()}}function x0(e,n){for(let t=Fb(e);t!==null;t=Pb(t))for(let i=Ve;i<t.length;i++){let r=t[i];S0(r,n)}}function nA(e){for(let n=Fb(e);n!==null;n=Pb(n)){if(!(n[$]&2))continue;let t=n[mr];for(let i=0;i<t.length;i++){let r=t[i];Uh(r)}}}function iA(e,n,t){ve(de.ComponentStart);let i=Ht(n,e);try{S0(i,t)}finally{ve(de.ComponentEnd,i[$e])}}function S0(e,n){_l(e)&&Lp(e,n)}function Lp(e,n){let i=e[V],r=e[$],o=e[Nt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Wr(o)),s||=!1,o&&(o.dirty=!1),e[$]&=-9217,s)tA(i,e,i.template,e[$e]);else if(r&8192){let a=O(null);try{D0(e),x0(e,1);let c=i.components;c!==null&&M0(e,c,1),w0(e)}finally{O(a)}}}function M0(e,n,t){for(let i=0;i<n.length;i++)iA(e,n[i],t)}function rA(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)ki(~r);else{let o=r,s=t[++i],a=t[++i];E_(s,o);let c=n[o];ve(de.HostBindingsUpdateStart,c);try{a(2,c)}finally{ve(de.HostBindingsUpdateEnd,c)}}}}finally{ki(-1)}}function Sm(e,n){let t=Jh()?64:1088;for(e[dn].changeDetectionScheduler?.notify(n);e;){e[$]|=t;let i=Ii(e);if(co(e)&&!i)return e;e=i}return null}function T0(e,n,t,i){return[e,!0,0,n,null,i,null,t,null,null]}function A0(e,n){let t=Ve+n;if(t<e.length)return e[t]}function Js(e,n,t,i=!0){let r=n[V];if(oA(r,n,e,t),i){let s=Pp(t,e),a=n[Me],c=a.parentNode(e[Ai]);c!==null&&wT(r,e[Dt],a,n,c,s)}let o=n[ur];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function R0(e,n){let t=Gs(e,n);return t!==void 0&&ad(t[V],t),t}function Gs(e,n){if(e.length<=Ve)return;let t=Ve+n,i=e[t];if(i){let r=i[Mi];r!==null&&r!==e&&wm(r,i),n>0&&(e[t-1][Bt]=i[Bt]);let o=Ts(e,Ve+n);bT(i[V],i);let s=o[An];s!==null&&s.detachView(o[V]),i[Ke]=null,i[Bt]=null,i[$]&=-129}return i}function oA(e,n,t,i){let r=Ve+i,o=t.length;i>0&&(t[r-1][Bt]=n),i<o-Ve?(n[Bt]=t[r],Sh(t,Ve+i,n)):(t.push(n),n[Bt]=null),n[Ke]=t;let s=n[Mi];s!==null&&t!==s&&k0(s,n);let a=n[An];a!==null&&a.insertView(e),bl(n),n[$]|=128}function k0(e,n){let t=e[mr],i=n[Ke];if(Xn(i))e[$]|=2;else{let r=i[Ke][It];n[It]!==r&&(e[$]|=2)}t===null?e[mr]=[n]:t.push(n)}var Ni=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[V];return zs(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[$e]}set context(n){this._lView[$e]=n}get destroyed(){return gr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Ke];if(un(n)){let t=n[Rs],i=t?t.indexOf(this):-1;i>-1&&(Gs(n,i),Ts(t,i))}this._attachedToViewContainer=!1}ad(this._lView[V],this._lView)}onDestroy(n){Hh(this._lView,n)}markForCheck(){Sm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[$]&=-129}reattach(){bl(this._lView),this._lView[$]|=128}detectChanges(){this._lView[$]|=1024,I0(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new x(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=co(this._lView),t=this._lView[Mi];t!==null&&!n&&wm(t,this._lView),l0(this._lView[V],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new x(902,!1);this._appRef=n;let t=co(this._lView),i=this._lView[Mi];i!==null&&!t&&k0(i,this._lView),bl(this._lView)}};var On=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=sA;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let o=Xs(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Ni(o)}}return e})();function sA(){return dd(tt(),z())}function dd(e,n){return e.type&4?new On(n,e,bo(e,n)):null}function Eo(e,n,t,i,r){let o=e.data[n];if(o===null)o=aA(e,n,t,i,r),w_()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=i,o.attrs=r;let s=y_();o.injectorIndex=s===null?-1:s.injectorIndex}return lo(o,!0),o}function aA(e,n,t,i,r){let o=Kh(),s=Qh(),a=s?o:o&&o.parent,c=e.data[n]=lA(e,a,t,n,i,r);return cA(e,c,o,s),c}function cA(e,n,t,i){e.firstChild===null&&(e.firstChild=n),t!==null&&(i?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function lA(e,n,t,i,r,o){let s=n?n.injectorIndex:-1,a=0;return qh()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:ip(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function dA(e){let n=e[Fh]??[],i=e[Ke][Me],r=[];for(let o of n)o.data[Vb]!==void 0?r.push(o):uA(o,i);e[Fh]=r}function uA(e,n){let t=0,i=e.firstChild;if(i){let r=e.data[jb];for(;t<r;){let o=i.nextSibling;Jb(n,i,!1),i=o,t++}}}var fA=()=>null,hA=()=>null;function zl(e,n){return fA(e,n)}function N0(e,n,t){return hA(e,n,t)}var O0=class{},ud=class{},jp=class{resolveComponentFactory(n){throw new x(917,!1)}},ea=class{static NULL=new jp},ut=class{},it=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>pA()}return e})();function pA(){let e=z(),n=tt(),t=Ht(n.index,e);return(Xn(t)?t:e)[Me]}var F0=(()=>{class e{static \u0275prov=E({token:e,providedIn:"root",factory:()=>null})}return e})();var Ll={},Vp=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,i){let r=this.injector.get(n,Ll,i);return r!==Ll||t===Ll?r:this.parentInjector.get(n,t,i)}};function Gl(e,n,t){let i=t?e.styles:null,r=t?e.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=cl(r,a);else if(o==2){let c=a,l=n[++s];i=cl(i,c+": "+l+";")}}t?e.styles=i:e.stylesWithoutHost=i,t?e.classes=r:e.classesWithoutHost=r}function ee(e,n=0){let t=z();if(t===null)return I(e,n);let i=tt();return Tb(i,t,lt(e),n)}function P0(e,n,t,i,r){let o=i===null?null:{"":-1},s=r(e,t);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}vA(e,n,t,a,o,c,l)}o!==null&&i!==null&&mA(t,i,o)}function mA(e,n,t){let i=e.localNames=[];for(let r=0;r<n.length;r+=2){let o=t[n[r+1]];if(o==null)throw new x(-301,!1);i.push(n[r],o)}}function gA(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function vA(e,n,t,i,r,o,s){let a=i.length,c=null;for(let h=0;h<a;h++){let p=i[h];c===null&&Rn(p)&&(c=p,gA(e,t,h)),Ep(Hl(t,n),e,p.type)}CA(t,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let p=i[h];p.providersResolver&&p.providersResolver(p)}let l=!1,d=!1,u=o0(e,n,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let p=i[h];if(t.mergedAttrs=mo(t.mergedAttrs,p.hostAttrs),_A(e,t,n,u,p),EA(u,p,r),s!==null&&s.has(p)){let[b,D]=s.get(p);t.directiveToIndex.set(p.type,[u,b+t.directiveStart,D+t.directiveStart])}else(o===null||!o.has(p))&&t.directiveToIndex.set(p.type,u);p.contentQueries!==null&&(t.flags|=4),(p.hostBindings!==null||p.hostAttrs!==null||p.hostVars!==0)&&(t.flags|=64);let m=p.type.prototype;!l&&(m.ngOnChanges||m.ngOnInit||m.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(m.ngOnChanges||m.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),u++}yA(e,t,o)}function yA(e,n,t){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=e.data[i];if(t===null||!t.has(r))eb(0,n,r,i),eb(1,n,r,i),nb(n,i,!1);else{let o=t.get(r);tb(0,n,o,i),tb(1,n,o,i),nb(n,i,!0)}}}function eb(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;e===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),L0(n,o)}}function tb(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;e===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),L0(n,s)}}function L0(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function nb(e,n,t){let{attrs:i,inputs:r,hostDirectiveInputs:o}=e;if(i===null||!t&&r===null||t&&o===null||pm(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function _A(e,n,t,i,r){e.data[i]=r;let o=r.factory||(r.factory=Ci(r.type,!0)),s=new _r(o,Rn(r),ee,null);e.blueprint[i]=s,t[i]=s,bA(e,n,i,o0(e,t,r.hostVars,Yt),r)}function bA(e,n,t,i,r){let o=r.hostBindings;if(o){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~n.index;wA(s)!=a&&s.push(a),s.push(t,i,o)}}function wA(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function EA(e,n,t){if(t){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)t[n.exportAs[i]]=e;Rn(n)&&(t[""]=e)}}function CA(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function j0(e,n,t,i,r,o,s,a){let c=n[V],l=c.consts,d=$t(l,s),u=Eo(c,e,t,i,d);return o&&P0(c,n,u,$t(l,a),r),u.mergedAttrs=mo(u.mergedAttrs,u.attrs),u.attrs!==null&&Gl(u,u.attrs,!1),u.mergedAttrs!==null&&Gl(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function V0(e,n){bb(e,n),Ph(n)&&e.queries.elementEnd(n)}function DA(e,n,t,i,r,o){let s=n.consts,a=$t(s,r),c=Eo(n,e,t,i,a);if(c.mergedAttrs=mo(c.mergedAttrs,c.attrs),o!=null){let l=$t(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Gl(c,c.attrs,!1),c.mergedAttrs!==null&&Gl(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function B0(e,n,t){return e[n]=t}function qt(e,n,t){if(t===Yt)return!1;let i=e[n];return Object.is(i,t)?!1:(e[n]=t,!0)}function IA(e,n,t,i){let r=qt(e,n,t);return qt(e,n+1,i)||r}function fp(e,n,t){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&xM(r,o);let s=Jn(e)?Ht(e.index,n):n;Sm(s,5);let a=n[$e],c=ib(n,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=ib(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function ib(e,n,t,i){let r=O(null);try{return ve(de.OutputStart,n,t),t(i)!==!1}catch(o){return $T(e,o),!1}finally{ve(de.OutputEnd,n,t),O(r)}}function xA(e,n,t,i,r,o,s,a){let c=ks(e),l=!1,d=null;if(!i&&c&&(d=MA(n,t,o,e.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=fn(e,t),h=i?i(u):u;MM(t,h,o,a),i||(a.__ngNativeEl__=u);let p=r.listen(h,o,a);if(!SA(o)){let m=i?b=>i(Ut(b[e.index])):e.index;U0(m,n,t,o,a,p,!1)}}return l}function SA(e){return e.startsWith("animation")||e.startsWith("transition")}function MA(e,n,t,i){let r=e.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===t&&r[o+1]===i){let a=n[so],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function U0(e,n,t,i,r,o,s){let a=n.firstCreatePass?zh(n):null,c=$h(t),l=c.length;c.push(r,o),a&&a.push(i,e,l,(l+1)*(s?-1:1))}function rb(e,n,t,i,r,o){let s=n[t],a=n[V],l=a.data[t].outputs[i],u=s[l].subscribe(o);U0(e.index,a,n,r,o,u,!0)}var Bp=Symbol("BINDING");function H0(e){return e.debugInfo?.className||e.type.name||null}var Wl=class extends ea{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let t=Zn(n);return new wr(t,this.ngModule)}};function TA(e){return Object.keys(e).map(n=>{let[t,i,r]=e[n],o={propName:t,templateName:n,isSignal:(i&rd.SignalBased)!==0};return r&&(o.transform=r),o})}function AA(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function RA(e,n,t){let i=n instanceof Ee?n:n?.injector;return i&&e.getStandaloneInjector!==null&&(i=e.getStandaloneInjector(i)||i),i?new Vp(t,i):t}function kA(e){let n=e.get(ut,null);if(n===null)throw new x(407,!1);let t=e.get(F0,null),i=e.get(Sn,null),r=e.get(vn,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function NA(e,n){let t=$0(e);return Qb(n,t,t==="svg"?Lh:t==="math"?d_:null)}function $0(e){return(e.selectors[0][0]||"div").toLowerCase()}var wr=class extends ud{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=TA(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=AA(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){super(),this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=cT(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,i,r,o,s){ve(de.DynamicComponentStart);let a=O(null);try{let c=this.componentDef,l=RA(c,r||this.ngModule,n),d=kA(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(H0(c),()=>this.createComponentRef(d,l,t,i,o,s)):this.createComponentRef(d,l,t,i,o,s)}finally{O(a)}}createComponentRef(n,t,i,r,o,s){let a=this.componentDef,c=OA(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?kT(l,r,a.encapsulation,t):NA(a,l),u=s?.some(ob)||o?.some(m=>typeof m!="function"&&m.bindings.some(ob)),h=gm(null,c,null,512|r0(a),null,null,n,l,t,null,Hb(d,t,!0));h[Oe]=d,Dl(h);let p=null;try{let m=j0(Oe,h,2,"#host",()=>c.directiveRegistry,!0,0);e0(l,d,m),go(d,h),Dm(c,h,m),zb(c,m,h),V0(c,m),i!==void 0&&PA(m,this.ngContentSelectors,i),p=Ht(m.index,h),h[$e]=p[$e],xm(c,h,null)}catch(m){throw p!==null&&Dp(p),Dp(h),m}finally{ve(de.DynamicComponentEnd),Il()}return new ql(this.componentType,h,!!u)}};function OA(e,n,t,i){let r=e?["ng-version","21.2.14"]:lT(n.selectors[0]),o=null,s=null,a=0;if(t)for(let d of t)a+=d[Bp].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let u=i[d];if(typeof u!="function")for(let h of u.bindings){a+=h[Bp].requiredVars;let p=d+1;h.create&&(h.targetIdx=p,(o??=[]).push(h)),h.update&&(h.targetIdx=p,(s??=[]).push(h))}}let c=[n];if(i)for(let d of i){let u=typeof d=="function"?d:d.type,h=Dh(u);c.push(h)}return mm(0,null,FA(o,s),1,a,c,null,null,null,[r],null)}function FA(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let i of e)i.create();if(t&2&&n)for(let i of n)i.update()}}function ob(e){let n=e[Bp].kind;return n==="input"||n==="twoWay"}var ql=class extends O0{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=vl(t[V],Oe),this.location=bo(this._tNode,t),this.instance=Ht(this._tNode.index,t)[$e],this.hostView=this.changeDetectorRef=new Ni(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let r=this._rootLView,o=Im(i,r[V],r,n,t);this.previousInputValues.set(n,t);let s=Ht(i.index,r);Sm(s,1)}get injector(){return new yr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function PA(e,n,t){let i=e.projection=[];for(let r=0;r<n.length;r++){let o=t[r];i.push(o!=null&&o.length?Array.from(o):null)}}var yn=(()=>{class e{static __NG_ELEMENT_ID__=LA}return e})();function LA(){let e=tt();return z0(e,z())}var Up=class e extends yn{_lContainer;_hostTNode;_hostLView;constructor(n,t,i){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=i}get element(){return bo(this._hostTNode,this._hostLView)}get injector(){return new yr(this._hostTNode,this._hostLView)}get parentInjector(){let n=rm(this._hostTNode,this._hostLView);if(Cb(n)){let t=Bl(n,this._hostLView),i=Vl(n),r=t[V].data[i+8];return new yr(r,t)}else return new yr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=sb(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-Ve}createEmbeddedView(n,t,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=zl(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,r,vo(this._hostTNode,s)),a}createComponent(n,t,i,r,o,s,a){let c=n&&!eM(n),l;if(c)l=t;else{let D=t||{};l=D.index,i=D.injector,r=D.projectableNodes,o=D.environmentInjector||D.ngModuleRef,s=D.directives,a=D.bindings}let d=c?n:new wr(Zn(n)),u=i||this.parentInjector;if(!o&&d.ngModule==null){let S=(c?u:this.parentInjector).get(Ee,null);S&&(o=S)}let h=Zn(d.componentType??{}),p=zl(this._lContainer,h?.id??null),m=p?.firstChild??null,b=d.create(u,r,m,o,s,a);return this.insertImpl(b.hostView,l,vo(this._hostTNode,p)),b}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,i){let r=n._lView;if(f_(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[Ke],l=new e(c,c[Dt],c[Ke]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(t),s=this._lContainer;return Js(s,r,o,i),n.attachToViewContainerRef(),Sh(hp(s),o,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=sb(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),i=Gs(this._lContainer,t);i&&(Ts(hp(this._lContainer),t),ad(i[V],i))}detach(n){let t=this._adjustIndex(n,-1),i=Gs(this._lContainer,t);return i&&Ts(hp(this._lContainer),t)!=null?new Ni(i):null}_adjustIndex(n,t=0){return n??this.length+t}};function sb(e){return e[Rs]}function hp(e){return e[Rs]||(e[Rs]=[])}function z0(e,n){let t,i=n[e.index];return un(i)?t=i:(t=T0(i,n,null,e),n[e.index]=t,vm(n,t)),VA(t,n,e,i),new Up(t,e,n)}function jA(e,n){let t=e[Me],i=t.createComment(""),r=fn(n,e),o=t.parentNode(r);return $l(t,o,i,t.nextSibling(r),!1),i}var VA=HA,BA=()=>!1;function UA(e,n,t){return BA(e,n,t)}function HA(e,n,t,i){if(e[Ai])return;let r;t.type&8?r=Ut(i):r=jA(n,t),e[Ai]=r}var Hp=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},$p=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let i=n.contentQueries!==null?n.contentQueries[0]:t.length,r=[];for(let o=0;o<i;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new e(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)Tm(n,t).matches!==null&&this.queries[t].setDirty()}},Yl=class{flags;read;predicate;constructor(n,t,i=null){this.flags=t,this.read=i,typeof n=="string"?this.predicate=qA(n):this.predicate=n}},zp=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,t!==null?t.push(o):t=[o])}return t!==null?new e(t):null}template(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Gp=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,t,$A(t,o)),this.matchTNodeWithReadOption(n,t,Pl(t,n,o,!1,!1))}else i===On?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,Pl(t,n,i,!1,!1))}matchTNodeWithReadOption(n,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Q||r===yn||r===On&&t.type&4)this.addMatch(t.index,-2);else{let o=Pl(t,n,r,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,i)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function $A(e,n){let t=e.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===n)return t[i+1]}return null}function zA(e,n){return e.type&11?bo(e,n):e.type&4?dd(e,n):null}function GA(e,n,t,i){return t===-1?zA(n,e):t===-2?WA(e,n,i):Hs(e,e[V],t,n)}function WA(e,n,t){if(t===Q)return bo(n,e);if(t===On)return dd(n,e);if(t===yn)return z0(n,e)}function G0(e,n,t,i){let r=n[An].queries[i];if(r.matches===null){let o=e.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(GA(n,d,s[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Wp(e,n,t,i){let r=e.queries.getByIndex(t),o=r.matches;if(o!==null){let s=G0(e,n,r,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let u=Ve;u<d.length;u++){let h=d[u];h[Mi]===h[Ke]&&Wp(h[V],h,l,i)}if(d[mr]!==null){let u=d[mr];for(let h=0;h<u.length;h++){let p=u[h];Wp(p[V],p,l,i)}}}}}return i}function Mm(e,n){return e[An].queries[n].queryList}function W0(e,n,t){let i=new ni((t&4)===4);return m_(e,n,i,i.destroy),(n[An]??=new $p).queries.push(new Hp(i))-1}function q0(e,n,t){let i=Le();return i.firstCreatePass&&(Z0(i,new Yl(e,n,t),-1),(n&2)===2&&(i.staticViewQueries=!0)),W0(i,z(),n)}function Y0(e,n,t,i){let r=Le();if(r.firstCreatePass){let o=tt();Z0(r,new Yl(n,t,i),o.index),YA(r,e),(t&2)===2&&(r.staticContentQueries=!0)}return W0(r,z(),t)}function qA(e){return e.split(",").map(n=>n.trim())}function Z0(e,n,t){e.queries===null&&(e.queries=new zp),e.queries.track(new Gp(n,t))}function YA(e,n){let t=e.contentQueries||(e.contentQueries=[]),i=t.length?t[t.length-1]:-1;n!==i&&t.push(e.queries.length-1,n)}function Tm(e,n){return e.queries.getByIndex(n)}function K0(e,n){let t=e[V],i=Tm(t,n);return i.crossesNgTemplate?Wp(t,e,n,[]):G0(t,e,i,n)}function Q0(e,n,t){let i,r=ls(()=>{i._dirtyCounter();let o=ZA(i,e);if(n&&o===void 0)throw new x(-951,!1);return o});return i=r[qe],i._dirtyCounter=Te(0),i._flatValue=void 0,r}function Am(e){return Q0(!0,!1,e)}function Rm(e){return Q0(!0,!0,e)}function X0(e,n){let t=e[qe];t._lView=z(),t._queryIndex=n,t._queryList=Mm(t._lView,n),t._queryList.onDirty(()=>t._dirtyCounter.update(i=>i+1))}function ZA(e,n){let t=e._lView,i=e._queryIndex;if(t===void 0||i===void 0||t[$]&4)return n?void 0:gt;let r=Mm(t,i),o=K0(t,i);return r.reset(o,kb),n?r.first:r._changesDetected||e._flatValue===void 0?e._flatValue=r.toArray():e._flatValue}var Fn=class{},fd=class{};var Zl=class extends Fn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Wl(this);constructor(n,t,i,r=!0){super(),this.ngModuleType=n,this._parent=t;let o=Ch(n);this._bootstrapComponents=t0(o.bootstrap),this._r3Injector=rp(n,t,[{provide:Fn,useValue:this},{provide:ea,useValue:this.componentFactoryResolver},...i],Ss(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Kl=class extends fd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Zl(this.moduleType,n,[])}};var Ws=class extends Fn{injector;componentFactoryResolver=new Wl(this);instance=null;constructor(n){super();let t=new lr([...n.providers,{provide:Fn,useValue:this},{provide:ea,useValue:this.componentFactoryResolver}],n.parent||oo(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ta(e,n,t=null){return new Ws({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var KA=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Ah(!1,t.type),r=i.length>0?ta([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=E({token:e,providedIn:"environment",factory:()=>new e(I(Ee))})}return e})();function K(e){return _o(()=>{let n=J0(e),t=A(_({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===sm.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(KA).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||gn.Emulated,styles:e.styles||gt,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&Fi("NgStandalone"),ew(t);let i=e.dependencies;return t.directiveDefs=ab(i,QA),t.pipeDefs=ab(i,Zy),t.id=eR(t),t})}function QA(e){return Zn(e)||Dh(e)}function X(e){return _o(()=>({type:e.type,bootstrap:e.bootstrap||gt,declarations:e.declarations||gt,imports:e.imports||gt,exports:e.exports||gt,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function XA(e,n){if(e==null)return xi;let t={};for(let i in e)if(e.hasOwnProperty(i)){let r=e[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=rd.None,c=null),t[o]=[i,a,c],n[o]=s}return t}function JA(e){if(e==null)return xi;let n={};for(let t in e)e.hasOwnProperty(t)&&(n[e[t]]=t);return n}function B(e){return _o(()=>{let n=J0(e);return ew(n),n})}function na(e){return{type:e.type,name:e.name,factory:null,pure:e.pure!==!1,standalone:e.standalone??!0,onDestroy:e.type.prototype.ngOnDestroy||null}}function J0(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||xi,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||gt,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:XA(e.inputs,n),outputs:JA(e.outputs),debugInfo:null}}function ew(e){e.features?.forEach(n=>n(e))}function ab(e,n){return e?()=>{let t=typeof e=="function"?e():e,i=[];for(let r of t){let o=n(r);o!==null&&i.push(o)}return i}:null}function eR(e){let n=0,t=typeof e.consts=="function"?"":e.consts,i=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function tR(e){return Object.getPrototypeOf(e.prototype).constructor}function Qe(e){let n=tR(e.type),t=!0,i=[e];for(;n;){let r;if(Rn(e))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new x(903,!1);r=n.\u0275dir}if(r){if(t){i.push(r);let s=e;s.inputs=pp(e.inputs),s.declaredInputs=pp(e.declaredInputs),s.outputs=pp(e.outputs);let a=r.hostBindings;a&&sR(e,a);let c=r.viewQuery,l=r.contentQueries;if(c&&rR(e,c),l&&oR(e,l),nR(e,r),Yy(e.outputs,r.outputs),Rn(r)&&r.data.animation){let d=e.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(e),a===Qe&&(t=!1)}}n=Object.getPrototypeOf(n)}iR(i)}function nR(e,n){for(let t in n.inputs){if(!n.inputs.hasOwnProperty(t)||e.inputs.hasOwnProperty(t))continue;let i=n.inputs[t];i!==void 0&&(e.inputs[t]=i,e.declaredInputs[t]=n.declaredInputs[t])}}function iR(e){let n=0,t=null;for(let i=e.length-1;i>=0;i--){let r=e[i];r.hostVars=n+=r.hostVars,r.hostAttrs=mo(r.hostAttrs,t=mo(t,r.hostAttrs))}}function pp(e){return e===xi?{}:e===gt?[]:e}function rR(e,n){let t=e.viewQuery;t?e.viewQuery=(i,r)=>{n(i,r),t(i,r)}:e.viewQuery=n}function oR(e,n){let t=e.contentQueries;t?e.contentQueries=(i,r,o)=>{n(i,r,o),t(i,r,o)}:e.contentQueries=n}function sR(e,n){let t=e.hostBindings;t?e.hostBindings=(i,r)=>{n(i,r),t(i,r)}:e.hostBindings=n}function tw(e,n,t,i,r,o,s,a){if(t.firstCreatePass){e.mergedAttrs=mo(e.mergedAttrs,e.attrs);let d=e.tView=mm(2,e,r,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}a&&(e.flags|=a),lo(e,!1);let c=cR(t,n,e,i);xl()&&Em(t,n,c,e),go(c,n);let l=T0(c,n,c,e);n[i+Oe]=l,vm(n,l),UA(l,e,n)}function aR(e,n,t,i,r,o,s,a,c,l,d){let u=t+Oe,h;return n.firstCreatePass?(h=Eo(n,u,4,s||null,a||null),Wh()&&P0(n,e,h,$t(n.consts,l),v0),bb(n,h)):h=n.data[u],tw(h,e,n,t,i,r,o,c),ks(h)&&Dm(n,e,h),l!=null&&ld(e,h,d),h}function qs(e,n,t,i,r,o,s,a,c,l,d){let u=t+Oe,h;if(n.firstCreatePass){if(h=Eo(n,u,4,s||null,a||null),l!=null){let p=$t(n.consts,l);h.localNames=[];for(let m=0;m<p.length;m+=2)h.localNames.push(p[m],-1)}}else h=n.data[u];return tw(h,e,n,t,i,r,o,c),l!=null&&ld(e,h,d),h}function Pi(e,n,t,i,r,o,s,a){let c=z(),l=Le(),d=$t(l.consts,o);return aR(c,l,e,n,t,i,r,d,void 0,s,a),Pi}var cR=lR;function lR(e,n,t,i){return Sl(!0),n[Me].createComment("")}var hd=(()=>{class e{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"platform"})}return e})();function Li(e){return typeof e=="function"&&e[qe]!==void 0}function km(e){return Li(e)&&typeof e.set=="function"}var Nm=new y("");function oi(e){return!!e&&typeof e.then=="function"}function pd(e){return!!e&&typeof e.subscribe=="function"}var nw=new y("");var Om=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=f(nw,{optional:!0})??[];injector=f(re);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let o=et(this.injector,r);if(oi(o))t.push(o);else if(pd(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),md=new y("");function iw(){Of(()=>{let e="";throw new x(600,e)})}function rw(e){return e.isBoundToModule}var dR=10;var Kt=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=f(Wt);afterRenderManager=f(sd);zonelessEnabled=f(Ls);rootEffectScheduler=f(Tl);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new M;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=f(ti);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(T(t=>!t))}constructor(){f(vn,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=f(Ee);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=re.NULL){return this._injector.get(j).run(()=>{ve(de.BootstrapComponentStart);let s=t instanceof ud;if(!this._injector.get(Om).done){let m="";throw new x(405,m)}let c;s?c=t:c=this._injector.get(ea).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=rw(c)?void 0:this._injector.get(Fn),d=i||c.selector,u=c.create(r,[],d,l),h=u.location.nativeElement,p=u.injector.get(Nm,null);return p?.registerApplication(h),u.onDestroy(()=>{this.detachView(u.hostView),Us(this.components,u),p?.unregisterApplication(h)}),this._loadComponent(u),ve(de.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ve(de.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(od.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ve(de.ChangeDetectionEnd),new x(101,!1);let t=O(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,O(t),this.afterTick.next(),ve(de.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ut,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<dR;){ve(de.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ve(de.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ns(r))continue;let o=i&&!this.zonelessEnabled?0:1;I0(r,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Ns(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Us(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(md,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Us(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new x(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Us(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function be(e,n,t,i){let r=z(),o=Ri();if(qt(r,o,n)){let s=Le(),a=Fs();BT(a,r,e,n,t,i)}return be}var qp=class{destroy(n){}updateValue(n,t){}swap(n,t){let i=Math.min(n,t),r=Math.max(n,t),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,t){this.attach(t,this.detach(n))}};function mp(e,n,t,i,r){return e===t&&Object.is(n,i)?1:Object.is(r(e,n),r(t,i))?-1:0}function uR(e,n,t,i){let r,o,s=0,a=e.length-1,c=void 0;if(Array.isArray(n)){O(i);let l=n.length-1;for(O(null);s<=a&&s<=l;){let d=e.at(s),u=n[s],h=mp(s,d,s,u,t);if(h!==0){h<0&&e.updateValue(s,u),s++;continue}let p=e.at(a),m=n[l],b=mp(a,p,l,m,t);if(b!==0){b<0&&e.updateValue(a,m),a--,l--;continue}let D=t(s,d),S=t(a,p),L=t(s,u);if(Object.is(L,S)){let ue=t(l,m);Object.is(ue,D)?(e.swap(s,a),e.updateValue(a,m),l--,a--):e.move(a,s),e.updateValue(s,u),s++;continue}if(r??=new Ql,o??=lb(e,s,a,t),Yp(e,r,s,L))e.updateValue(s,u),s++,a++;else if(o.has(L))r.set(D,e.detach(s)),a--;else{let ue=e.create(s,n[s]);e.attach(s,ue),s++,a++}}for(;s<=l;)cb(e,r,t,s,n[s]),s++}else if(n!=null){O(i);let l=n[Symbol.iterator]();O(null);let d=l.next();for(;!d.done&&s<=a;){let u=e.at(s),h=d.value,p=mp(s,u,s,h,t);if(p!==0)p<0&&e.updateValue(s,h),s++,d=l.next();else{r??=new Ql,o??=lb(e,s,a,t);let m=t(s,h);if(Yp(e,r,s,m))e.updateValue(s,h),s++,a++,d=l.next();else if(!o.has(m))e.attach(s,e.create(s,h)),s++,a++,d=l.next();else{let b=t(s,u);r.set(b,e.detach(s)),a--}}}for(;!d.done;)cb(e,r,t,e.length,d.value),d=l.next()}for(;s<=a;)e.destroy(e.detach(a--));r?.forEach(l=>{e.destroy(l)})}function Yp(e,n,t,i){return n!==void 0&&n.has(i)?(e.attach(t,n.get(i)),n.delete(i),!0):!1}function cb(e,n,t,i,r){if(Yp(e,n,i,t(i,r)))e.updateValue(i,r);else{let o=e.create(i,r);e.attach(i,o)}}function lb(e,n,t,i){let r=new Set;for(let o=n;o<=t;o++)r.add(i(o,e.at(o)));return r}var Ql=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let t=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(n,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,t){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(n,t)}forEach(n){for(let[t,i]of this.kvMap)if(n(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,t)}}};function k(e,n,t,i,r,o,s,a){Fi("NgControlFlow");let c=z(),l=Le(),d=$t(l.consts,o);return qs(c,l,e,n,t,i,r,d,256,s,a),Fm}function Fm(e,n,t,i,r,o,s,a){Fi("NgControlFlow");let c=z(),l=Le(),d=$t(l.consts,o);return qs(c,l,e,n,t,i,r,d,512,s,a),Fm}function N(e,n){Fi("NgControlFlow");let t=z(),i=Ri(),r=t[i]!==Yt?t[i]:-1,o=r!==-1?Xl(t,Oe+r):void 0,s=0;if(qt(t,i,e)){let a=O(null);try{if(o!==void 0&&R0(o,s),e!==-1){let c=Oe+e,l=Xl(t,c),d=Xp(t[V],c),u=N0(l,d,t),h=Xs(t,d,n,{dehydratedView:u});Js(l,h,s,vo(d,u))}}finally{O(a)}}else if(o!==void 0){let a=A0(o,s);a!==void 0&&(a[$e]=n)}}var Zp=class{lContainer;$implicit;$index;constructor(n,t,i){this.lContainer=n,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Ve}};function Qt(e){return e}var Kp=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,t,i){this.hasEmptyBlock=n,this.trackByFn=t,this.liveCollection=i}};function Xt(e,n,t,i,r,o,s,a,c,l,d,u,h){Fi("NgControlFlow");let p=z(),m=Le(),b=c!==void 0,D=z(),S=a?s.bind(D[It][$e]):s,L=new Kp(b,S);D[Oe+e]=L,qs(p,m,e+1,n,t,i,r,$t(m.consts,o),256),b&&qs(p,m,e+2,c,l,d,u,$t(m.consts,h),512)}var Qp=class extends qp{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,t,i){super(),this.lContainer=n,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Ve}at(n){return this.getLView(n)[$e].$implicit}attach(n,t){let i=t[ur];this.needsIndexUpdate||=n!==this.length,Js(this.lContainer,t,n,vo(this.templateTNode,i)),fR(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,hR(this.lContainer,n),pR(this.lContainer,n)}create(n,t){let i=zl(this.lContainer,this.templateTNode.tView.ssrId);return Xs(this.hostLView,this.templateTNode,new Zp(this.lContainer,t,n),{dehydratedView:i})}destroy(n){ad(n[V],n)}updateValue(n,t){this.getLView(n)[$e].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[$e].$index=n}getLView(n){return mR(this.lContainer,n)}};function Jt(e){let n=O(null),t=ei();try{let i=z(),r=i[V],o=i[t],s=t+1,a=Xl(i,s);if(o.liveCollection===void 0){let l=Xp(r,s);o.liveCollection=new Qp(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(uR(c,e,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=Ri(),d=c.length===0;if(qt(i,l,d)){let u=t+2,h=Xl(i,u);if(d){let p=Xp(r,u),m=N0(h,p,i),b=Xs(i,p,void 0,{dehydratedView:m});Js(h,b,0,vo(p,m))}else r.firstUpdatePass&&dA(h),R0(h,0)}}}finally{O(n)}}function Xl(e,n){return e[n]}function fR(e,n){if(e.length<=Ve)return;let t=Ve+n,i=e[t],r=i?i[Ti]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Kn];yT(o,r),br.delete(i[Qn]),r.detachedLeaveAnimationFns=void 0}}function hR(e,n){if(e.length<=Ve)return;let t=Ve+n,i=e[t],r=i?i[Ti]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function pR(e,n){return Gs(e,n)}function mR(e,n){return A0(e,n)}function Xp(e,n){return vl(e,n)}function ye(e,n,t){let i=z(),r=Ri();if(qt(i,r,n)){let o=Le(),s=Fs();m0(s,i,e,n,i[Me],t)}return ye}function Jp(e,n,t,i,r){Im(n,e,t,r?"class":"style",i)}function g(e,n,t,i){let r=z(),o=r[V],s=e+Oe,a=o.firstCreatePass?j0(s,r,2,n,v0,Wh(),t,i):o.data[s];if(Jn(a)){let c=r[dn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(H0(l),()=>(db(e,n,r,a,i),g))}}return db(e,n,r,a,i),g}function db(e,n,t,i,r){if(y0(i,t,e,n,ow),ks(i)){let o=t[V];Dm(o,t,i),zb(o,i,t)}r!=null&&ld(t,i)}function v(){let e=Le(),n=tt(),t=_0(n);return e.firstCreatePass&&V0(e,t),Yh(t)&&Zh(),Gh(),t.classesWithoutHost!=null&&sM(t)&&Jp(e,t,z(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&aM(t)&&Jp(e,t,z(),t.stylesWithoutHost,!1),v}function Ce(e,n,t,i){return g(e,n,t,i),v(),Ce}function rt(e,n,t,i){let r=z(),o=r[V],s=e+Oe,a=o.firstCreatePass?DA(s,o,2,n,t,i):o.data[s];return y0(a,r,e,n,ow),i!=null&&ld(r,a),rt}function ot(){let e=tt(),n=_0(e);return Yh(n)&&Zh(),Gh(),ot}function At(e,n,t,i){return rt(e,n,t,i),ot(),At}var ow=(e,n,t,i,r)=>(Sl(!0),Qb(n[Me],i,ip()));function _n(){return z()}function Ln(e,n,t){let i=z(),r=Ri();if(qt(i,r,n)){let o=Le(),s=Fs();g0(s,i,e,n,i[Me],t)}return Ln}var js=void 0;function gR(e){let n=Math.floor(Math.abs(e)),t=e.toString().replace(/^[^.]*\.?/,"").length;return n===1&&t===0?1:5}var vR=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],js,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],js,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",js,js,js],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",gR],gp={};function Ot(e){let n=yR(e),t=ub(n);if(t)return t;let i=n.split("-")[0];if(t=ub(i),t)return t;if(i==="en")return vR;throw new x(701,!1)}function ub(e){return e in gp||(gp[e]=Tn.ng&&Tn.ng.common&&Tn.ng.common.locales&&Tn.ng.common.locales[e]),gp[e]}var ze=(function(e){return e[e.LocaleId=0]="LocaleId",e[e.DayPeriodsFormat=1]="DayPeriodsFormat",e[e.DayPeriodsStandalone=2]="DayPeriodsStandalone",e[e.DaysFormat=3]="DaysFormat",e[e.DaysStandalone=4]="DaysStandalone",e[e.MonthsFormat=5]="MonthsFormat",e[e.MonthsStandalone=6]="MonthsStandalone",e[e.Eras=7]="Eras",e[e.FirstDayOfWeek=8]="FirstDayOfWeek",e[e.WeekendRange=9]="WeekendRange",e[e.DateFormat=10]="DateFormat",e[e.TimeFormat=11]="TimeFormat",e[e.DateTimeFormat=12]="DateTimeFormat",e[e.NumberSymbols=13]="NumberSymbols",e[e.NumberFormats=14]="NumberFormats",e[e.CurrencyCode=15]="CurrencyCode",e[e.CurrencySymbol=16]="CurrencySymbol",e[e.CurrencyName=17]="CurrencyName",e[e.Currencies=18]="Currencies",e[e.Directionality=19]="Directionality",e[e.PluralCase=20]="PluralCase",e[e.ExtraData=21]="ExtraData",e})(ze||{});function yR(e){return e.toLowerCase().replace(/_/g,"-")}var ia="en-US";var _R=ia;function sw(e){typeof e=="string"&&(_R=e.toLowerCase().replace(/_/g,"-"))}function pe(e,n,t){let i=z(),r=Le(),o=tt();return aw(r,i,i[Me],o,e,n,t),pe}function aw(e,n,t,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=fp(i,n,o),xA(i,e,n,s,t,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let u=0;u<d.length;u+=2){let h=d[u],p=d[u+1];c??=fp(i,n,o),rb(i,n,h,p,r,c)}if(l&&l.length)for(let u of l)c??=fp(i,n,o),rb(i,n,u,r,r,c)}}function q(e=1){return M_(e)}function bR(e,n){let t=null,i=iT(e);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){t=r;continue}if(i===null?i0(e,o,!0):sT(i,o))return r}return t}function Be(e){let n=z()[It][Dt];if(!n.projection){let t=e?e.length:1,i=n.projection=n_(t,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=e?bR(o,e):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function se(e,n=0,t,i,r,o){let s=z(),a=Le(),c=i?e+1:null;c!==null&&qs(s,a,c,i,r,o,null,t);let l=Eo(a,Oe+e,16,null,t||null);l.projection===null&&(l.projection=n),Xh();let u=!s[ur]||qh();s[It][Dt].projection[l.projection]===null&&c!==null?wR(s,a,c):u&&!nd(l)&&TT(a,s,l)}function wR(e,n,t){let i=Oe+t,r=n.data[i],o=e[i],s=zl(o,r.tView.ssrId),a=Xs(e,r,void 0,{dehydratedView:s});Js(o,a,0,vo(r,s))}function si(e,n,t,i){return Y0(e,n,t,i),si}function bn(e,n,t){return q0(e,n,t),bn}function ce(e){let n=z(),t=Le(),i=Cl();Os(i+1);let r=Tm(t,i);if(e.dirty&&u_(n)===((r.metadata.flags&2)===2)){if(r.matches===null)e.reset([]);else{let o=K0(n,i);e.reset(o,kb),e.notifyOnChanges()}return!0}return!1}function le(){return Mm(z(),Cl())}function gd(e,n,t,i,r){return X0(n,Y0(e,t,i,r)),gd}function vd(e,n,t,i){return X0(e,q0(n,t,i)),vd}function yd(e=1){Os(Cl()+e)}function Co(e){let n=__();return yl(n,Oe+e)}function Nl(e,n){return e<<17|n<<2}function Er(e){return e>>17&32767}function ER(e){return(e&2)==2}function CR(e,n){return e&131071|n<<17}function em(e){return e|2}function yo(e){return(e&131068)>>2}function vp(e,n){return e&-131069|n<<2}function DR(e){return(e&1)===1}function tm(e){return e|1}function IR(e,n,t,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Er(s),c=yo(s);e[i]=t;let l=!1,d;if(Array.isArray(t)){let u=t;d=u[1],(d===null||io(u,d)>0)&&(l=!0)}else d=t;if(r)if(c!==0){let h=Er(e[a+1]);e[i+1]=Nl(h,a),h!==0&&(e[h+1]=vp(e[h+1],i)),e[a+1]=CR(e[a+1],i)}else e[i+1]=Nl(a,0),a!==0&&(e[a+1]=vp(e[a+1],i)),a=i;else e[i+1]=Nl(c,0),a===0?a=i:e[c+1]=vp(e[c+1],i),c=i;l&&(e[i+1]=em(e[i+1])),fb(e,d,i,!0),fb(e,d,i,!1),xR(n,d,e,i,o),s=Nl(a,c),o?n.classBindings=s:n.styleBindings=s}function xR(e,n,t,i,r){let o=r?e.residualClasses:e.residualStyles;o!=null&&typeof n=="string"&&io(o,n)>=0&&(t[i+1]=tm(t[i+1]))}function fb(e,n,t,i){let r=e[t+1],o=n===null,s=i?Er(r):yo(r),a=!1;for(;s!==0&&(a===!1||o);){let c=e[s],l=e[s+1];SR(c,n)&&(a=!0,e[s+1]=i?tm(l):em(l)),s=i?Er(l):yo(l)}a&&(e[t+1]=i?em(r):tm(r))}function SR(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?io(e,n)>=0:!1}var mn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function MR(e){return e.substring(mn.key,mn.keyEnd)}function TR(e){return AR(e),cw(e,lw(e,0,mn.textEnd))}function cw(e,n){let t=mn.textEnd;return t===n?-1:(n=mn.keyEnd=RR(e,mn.key=n,t),lw(e,n,t))}function AR(e){mn.key=0,mn.keyEnd=0,mn.value=0,mn.valueEnd=0,mn.textEnd=e.length}function lw(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function RR(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function ra(e,n,t){return dw(e,n,t,!1),ra}function te(e,n){return dw(e,n,null,!0),te}function wn(e){NR(VR,kR,e,!0)}function kR(e,n){for(let t=TR(n);t>=0;t=cw(n,t))pl(e,MR(n),!0)}function dw(e,n,t,i){let r=z(),o=Le(),s=ep(2);if(o.firstUpdatePass&&fw(o,e,s,i),n!==Yt&&qt(r,s,n)){let a=o.data[ei()];hw(o,a,r,r[Me],e,r[s+1]=UR(n,t),i,s)}}function NR(e,n,t,i){let r=Le(),o=ep(2);r.firstUpdatePass&&fw(r,null,o,i);let s=z();if(t!==Yt&&qt(s,o,t)){let a=r.data[ei()];if(pw(a,i)&&!uw(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=cl(c,t||"")),Jp(r,a,s,t,i)}else BR(r,a,s,s[Me],s[o+1],s[o+1]=jR(e,n,t),i,o)}}function uw(e,n){return n>=e.expandoStartIndex}function fw(e,n,t,i){let r=e.data;if(r[t+1]===null){let o=r[ei()],s=uw(e,t);pw(o,i)&&n===null&&!s&&(n=!1),n=OR(r,o,n,i),IR(r,o,n,t,s,i)}}function OR(e,n,t,i){let r=D_(e),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(t=yp(null,e,n,t,i),t=Ys(t,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||e[s]!==r)if(t=yp(r,e,n,t,i),o===null){let c=FR(e,n,i);c!==void 0&&Array.isArray(c)&&(c=yp(null,e,n,c[1],i),c=Ys(c,n.attrs,i),PR(e,n,i,c))}else o=LR(e,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),t}function FR(e,n,t){let i=t?n.classBindings:n.styleBindings;if(yo(i)!==0)return e[Er(i)]}function PR(e,n,t,i){let r=t?n.classBindings:n.styleBindings;e[Er(r)]=i}function LR(e,n,t){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=e[o].hostAttrs;i=Ys(i,s,t)}return Ys(i,n.attrs,t)}function yp(e,n,t,i,r){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=n[a],i=Ys(i,o.hostAttrs,r),o!==e);)a++;return e!==null&&(t.directiveStylingLast=a),i}function Ys(e,n,t){let i=t?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),pl(e,s,t?!0:n[++o]))}return e===void 0?null:e}function jR(e,n,t){if(t==null||t==="")return gt;let i=[],r=Pn(t);if(Array.isArray(r))for(let o=0;o<r.length;o++)e(i,r[o],!0);else if(r instanceof Set)for(let o of r)e(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&e(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function VR(e,n,t){let i=String(n);i!==""&&!i.includes(" ")&&pl(e,i,t)}function BR(e,n,t,i,r,o,s,a){r===Yt&&(r=gt);let c=0,l=0,d=0<r.length?r[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let h=c<r.length?r[c+1]:void 0,p=l<o.length?o[l+1]:void 0,m=null,b;d===u?(c+=2,l+=2,h!==p&&(m=u,b=p)):u===null||d!==null&&d<u?(c+=2,m=d):(l+=2,m=u,b=p),m!==null&&hw(e,n,t,i,m,b,s,a),d=c<r.length?r[c]:null,u=l<o.length?o[l]:null}}function hw(e,n,t,i,r,o,s,a){if(!(n.type&3))return;let c=e.data,l=c[a+1],d=DR(l)?hb(c,n,t,r,yo(l),s):void 0;if(!Jl(d)){Jl(o)||ER(l)&&(o=hb(c,null,t,r,a,s));let u=jh(ei(),t);RT(i,s,u,r,o)}}function hb(e,n,t,i,r,o){let s=n===null,a;for(;r>0;){let c=e[r],l=Array.isArray(c),d=l?c[1]:c,u=d===null,h=t[r+1];h===Yt&&(h=u?gt:void 0);let p=u?ml(h,i):d===i?h:void 0;if(l&&!Jl(p)&&(p=ml(c,i)),Jl(p)&&(a=p,s))return a;let m=e[r+1];r=s?Er(m):yo(m)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=ml(c,i))}return a}function Jl(e){return e!==void 0}function UR(e,n){return e==null||e===""||(typeof n=="string"?e=e+n:typeof e=="object"&&(e=Ss(Pn(e)))),e}function pw(e,n){return(e.flags&(n?8:16))!==0}function C(e,n=""){let t=z(),i=Le(),r=e+Oe,o=i.firstCreatePass?Eo(i,r,1,n,null):i.data[r],s=HR(i,t,o,n);t[r]=s,xl()&&Em(i,t,s,o),lo(o,!1)}var HR=(e,n,t,i)=>(Sl(!0),qM(n[Me],i));function $R(e,n,t,i=""){return qt(e,Ri(),t)?n+ul(t)+i:Yt}function Re(e){return Fe("",e),Re}function Fe(e,n,t){let i=z(),r=$R(i,e,n,t);return r!==Yt&&zR(i,ei(),r),Fe}function zR(e,n,t){let i=jh(n,e);YM(e[Me],i,t)}function oa(e,n,t){km(n)&&(n=n());let i=z(),r=Ri();if(qt(i,r,n)){let o=Le(),s=Fs();m0(s,i,e,n,i[Me],t)}return oa}function _d(e,n){let t=km(e);return t&&e.set(n),t}function sa(e,n){let t=z(),i=Le(),r=tt();return aw(i,t,t[Me],r,e,n),sa}function pb(e,n,t){let i=Le();i.firstCreatePass&&mw(n,i.data,i.blueprint,Rn(e),t)}function mw(e,n,t,i,r){if(e=lt(e),Array.isArray(e))for(let o=0;o<e.length;o++)mw(e[o],n,t,i,r);else{let o=Le(),s=z(),a=tt(),c=cr(e)?e:lt(e.provide),l=kh(e),d=a.providerIndexes&1048575,u=a.directiveStart,h=a.providerIndexes>>20;if(cr(e)||!e.multi){let p=new _r(l,r,ee,null),m=bp(c,n,r?d:d+h,u);m===-1?(Ep(Hl(a,s),o,c),_p(o,e,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),s.push(p)):(t[m]=p,s[m]=p)}else{let p=bp(c,n,d+h,u),m=bp(c,n,d,d+h),b=p>=0&&t[p],D=m>=0&&t[m];if(r&&!D||!r&&!b){Ep(Hl(a,s),o,c);let S=qR(r?WR:GR,t.length,r,i,l,e);!r&&D&&(t[m].providerFactory=S),_p(o,e,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(S),s.push(S)}else{let S=gw(t[r?m:p],l,!r&&i);_p(o,e,p>-1?p:m,S)}!r&&i&&D&&t[m].componentProviders++}}}function _p(e,n,t,i){let r=cr(n),o=a_(n);if(r||o){let c=(o?lt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[i,c]):l[d+1].push(i,c)}else l.push(t,c)}}}function gw(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function bp(e,n,t,i){for(let r=t;r<i;r++)if(n[r]===e)return r;return-1}function GR(e,n,t,i,r){return nm(this.multi,[])}function WR(e,n,t,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Hs(i,i[V],this.providerFactory.index,r);s=c.slice(0,a),nm(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],nm(o,s);return s}function nm(e,n){for(let t=0;t<e.length;t++){let i=e[t];n.push(i())}return n}function qR(e,n,t,i,r,o){let s=new _r(e,t,ee,null);return s.multi=[],s.index=n,s.componentProviders=0,gw(s,r,i&&!t),s}function st(e,n){return t=>{t.providersResolver=(i,r)=>pb(i,r?r(e):e,!1),n&&(t.viewProvidersResolver=(i,r)=>pb(i,r?r(n):n,!0))}}function Pm(e,n,t){return yw(z(),wl(),e,n,t)}function vw(e,n){let t=e[n];return t===Yt?void 0:t}function yw(e,n,t,i,r,o){let s=n+t;return qt(e,s,r)?B0(e,s+1,o?i.call(o,r):i(r)):vw(e,s+1)}function YR(e,n,t,i,r,o,s){let a=n+t;return IA(e,a,r,o)?B0(e,a+2,s?i.call(s,r,o):i(r,o)):vw(e,a+2)}function jn(e,n){let t=Le(),i,r=e+Oe;t.firstCreatePass?(i=ZR(n,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let o=i.factory||(i.factory=Ci(i.type,!0)),s,a=Ct(ee);try{let c=Ul(!1),l=o();return Ul(c),Vh(t,z(),r,l),l}finally{Ct(a)}}function ZR(e,n){if(n)for(let t=n.length-1;t>=0;t--){let i=n[t];if(e===i.name)return i}}function aa(e,n,t){let i=e+Oe,r=z(),o=yl(r,i);return _w(r,i)?yw(r,wl(),n,o.transform,t,o):o.transform(t)}function Ir(e,n,t,i){let r=e+Oe,o=z(),s=yl(o,r);return _w(o,r)?YR(o,wl(),n,s.transform,t,i,s):s.transform(t,i)}function _w(e,n){return e[V].data[n].pure}function Lm(e,n){return dd(e,n)}var ed=class{ngModuleFactory;componentFactories;constructor(n,t){this.ngModuleFactory=n,this.componentFactories=t}},jm=(()=>{class e{compileModuleSync(t){return new Kl(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=Ch(t),o=t0(r.declarations).reduce((s,a)=>{let c=Zn(a);return c&&s.push(new wr(c)),s},[]);return new ed(i,o)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var bw=(()=>{class e{applicationErrorHandler=f(Wt);appRef=f(Kt);taskService=f(ti);ngZone=f(j);zonelessEnabled=f(Ls);tracing=f(vn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Ie;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Is):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(f(cp,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?k_:op;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Is+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function ww(){return[{provide:Sn,useExisting:bw},{provide:j,useClass:xs},{provide:Ls,useValue:!0}]}function KR(){return typeof $localize<"u"&&$localize.locale||ia}var ca=new y("",{factory:()=>f(ca,{optional:!0,skipSelf:!0})||KR()});var bd=class{destroyed=!1;listeners=null;errorHandler=f(Ye,{optional:!0});destroyRef=f(dt);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new x(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let t=this.listeners?.indexOf(n);t!==void 0&&t!==-1&&this.listeners?.splice(t,1)}}}emit(n){if(this.destroyed){console.warn(Mn(953,!1));return}if(this.listeners===null)return;let t=O(null);try{for(let i of this.listeners)try{i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{O(t)}}};function je(e){return $y(e)}function Ge(e,n){return ls(e,n?.equal)}var Ed=Symbol("InputSignalNode#UNSET"),Tw=A(_({},ds),{transformFn:void 0,applyValueToInputSignal(e,n){Yi(e,n)}});function Aw(e,n){let t=Object.create(Tw);t.value=e,t.transformFn=n?.transform;function i(){if(mi(t),t.value===Ed){let r=null;throw new x(-950,r)}return t.value}return i[qe]=t,i}var xr=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>om(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},Rw=(()=>{let e=new y("");return e.__NG_ELEMENT_ID__=n=>{let t=tt();if(t===null)throw new x(-204,!1);if(t.type&2)return t.value;if(n&8)return null;throw new x(-204,!1)},e})();function Ew(e,n){return Aw(e,n)}function pk(e){return Aw(Ed,e)}var kw=(Ew.required=pk,Ew);function Cw(e,n){return Am(n)}function mk(e,n){return Rm(n)}var da=(Cw.required=mk,Cw);function Dw(e,n){return Am(n)}function gk(e,n){return Rm(n)}var Nw=(Dw.required=gk,Dw);function Ow(e,n){let t=Object.create(Tw),i=new bd;t.value=e;function r(){return mi(t),Iw(t.value),t.value}return r[qe]=t,r.asReadonly=Ml.bind(r),r.set=o=>{t.equal(t.value,o)||(Yi(t,o),i.emit(o))},r.update=o=>{Iw(t.value),r.set(o(t.value))},r.subscribe=i.subscribe.bind(i),r.destroyRef=i.destroyRef,r}function Iw(e){if(e===Ed)throw new x(952,!1)}function xw(e,n){return Ow(e,n)}function vk(e){return Ow(Ed,e)}var $m=(xw.required=vk,xw);var Bm=new y(""),yk=new y("");function la(e){return!e.moduleRef}function _k(e){let n=la(e)?e.r3Injector:e.moduleRef.injector,t=n.get(j);return t.run(()=>{la(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let i=n.get(Wt),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),la(e)){let o=()=>n.destroy(),s=e.platformInjector.get(Bm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>e.moduleRef.destroy(),s=e.platformInjector.get(Bm);s.add(o),e.moduleRef.onDestroy(()=>{Us(e.allPlatformModules,e.moduleRef),r.unsubscribe(),s.delete(o)})}return wk(i,t,()=>{let o=n.get(ti),s=o.add(),a=n.get(Om);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(ca,ia);if(sw(c||ia),!n.get(yk,!0))return la(e)?n.get(Kt):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(la(e)){let d=n.get(Kt);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return bk?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{o.remove(s)})})})}var bk;function wk(e,n,t){try{let i=t();return oi(i)?i.catch(r=>{throw n.runOutsideAngular(()=>e(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>e(i)),i}}var wd=null;function Ek(e=[],n){return re.create({name:n,providers:[{provide:As,useValue:"platform"},{provide:Bm,useValue:new Set([()=>wd=null])},...e]})}function Ck(e=[]){if(wd)return wd;let n=Ek(e);return wd=n,iw(),Dk(n),n}function Dk(e){let n=e.get(td,null);et(e,()=>{n?.forEach(t=>t())})}function zm(){return!1}var Ik=1e4;var y9=Ik-1e3;var ht=(()=>{class e{static __NG_ELEMENT_ID__=xk}return e})();function xk(e){return Sk(tt(),z(),(e&16)===16)}function Sk(e,n,t){if(Jn(e)&&!t){let i=Ht(e.index,n);return new Ni(i,i)}else if(e.type&175){let i=n[It];return new Ni(i,n)}return null}function Fw(e){let{rootComponent:n,appProviders:t,platformProviders:i,platformRef:r}=e;ve(de.BootstrapApplicationStart);try{let o=r?.injector??Ck(i),s=[ww(),O_,...t||[]],a=new Ws({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return _k({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{ve(de.BootstrapApplicationEnd)}}function we(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Vn(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}var Vm=Symbol("NOT_SET"),Pw=new Set,Mk=A(_({},ds),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Vm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Vm&&!Wr(this))return this.signal;try{for(let r of this.cleanup??Pw)r()}finally{this.cleanup?.clear()}let n=[];e!==void 0&&n.push(e),n.push(this.registerCleanupFn);let t=gi(this),i;try{i=this.userFn.apply(null,n)}finally{qi(this,t)}return(this.value===Vm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Um=class extends $s{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,t,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(dt),s),this.scheduler=r;for(let a of _m){let c=t[a];if(c===void 0)continue;let l=Object.create(Mk);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(mi(l),l.value),l.signal[qe]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let t of n.cleanup??Pw)t()}finally{vi(n)}}};function Lw(e,n){let t=n?.injector??f(re),i=t.get(Sn),r=t.get(sd),o=t.get(vn,null,{optional:!0});r.impl??=t.get(bm);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=t.get(uo,null,{optional:!0}),c=new Um(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,t,o?.snapshot(null));return r.impl.register(c),c}function Cd(e,n){let t=Zn(e),i=n.elementInjector||oo();return new wr(t).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var jw=null;function en(){return jw}function Gm(e){jw??=e}var ua=class{},Do=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>f(Vw),providedIn:"platform"})}return e})();var Vw=(()=>{class e extends Do{_location;_history;_doc=f(W);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return en().getBaseHref(this._doc)}onPopState(t){let i=en().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=en().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>new e,providedIn:"platform"})}return e})();function Hw(e,n){return e?n?e.endsWith("/")?n.startsWith("/")?e+n.slice(1):e+n:n.startsWith("/")?e+n:`${e}/${n}`:e:n}function Bw(e){let n=e.search(/#|\?|$/);return e[n-1]==="/"?e.slice(0,n-1)+e.slice(n):e}function ji(e){return e&&e[0]!=="?"?`?${e}`:e}var Dd=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>f(Ak),providedIn:"root"})}return e})(),Tk=new y(""),Ak=(()=>{class e extends Dd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??f(W).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Hw(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+ji(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+ji(o));this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+ji(o));this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||e)(I(Do),I(Tk,8))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Vi=(()=>{class e{_subject=new M;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=Nk(Bw(Uw(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+ji(i))}normalize(t){return e.stripTrailingSlash(kk(this._basePath,Uw(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+ji(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+ji(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=ji;static joinWithSlash=Hw;static stripTrailingSlash=Bw;static \u0275fac=function(i){return new(i||e)(I(Dd))};static \u0275prov=E({token:e,factory:()=>Rk(),providedIn:"root"})}return e})();function Rk(){return new Vi(I(Dd))}function kk(e,n){if(!e||!n.startsWith(e))return n;let t=n.substring(e.length);return t===""||["/",";","?","#"].includes(t[0])?t:n}function Uw(e){return e.replace(/\/index.html$/,"")}function Nk(e){if(new RegExp("^(https?:)?//").test(e)){let[,t]=e.split(/\/\/[^\/]+/);return t}return e}var yt=(function(e){return e[e.Format=0]="Format",e[e.Standalone=1]="Standalone",e})(yt||{}),De=(function(e){return e[e.Narrow=0]="Narrow",e[e.Abbreviated=1]="Abbreviated",e[e.Wide=2]="Wide",e[e.Short=3]="Short",e})(De||{}),Rt=(function(e){return e[e.Short=0]="Short",e[e.Medium=1]="Medium",e[e.Long=2]="Long",e[e.Full=3]="Full",e})(Rt||{}),ci={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Gw(e){return Ot(e)[ze.LocaleId]}function Ww(e,n,t){let i=Ot(e),r=[i[ze.DayPeriodsFormat],i[ze.DayPeriodsStandalone]],o=tn(r,n);return tn(o,t)}function qw(e,n,t){let i=Ot(e),r=[i[ze.DaysFormat],i[ze.DaysStandalone]],o=tn(r,n);return tn(o,t)}function Yw(e,n,t){let i=Ot(e),r=[i[ze.MonthsFormat],i[ze.MonthsStandalone]],o=tn(r,n);return tn(o,t)}function Zw(e,n){let i=Ot(e)[ze.Eras];return tn(i,n)}function fa(e,n){let t=Ot(e);return tn(t[ze.DateFormat],n)}function ha(e,n){let t=Ot(e);return tn(t[ze.TimeFormat],n)}function pa(e,n){let i=Ot(e)[ze.DateTimeFormat];return tn(i,n)}function ma(e,n){let t=Ot(e),i=t[ze.NumberSymbols][n];if(typeof i>"u"){if(n===ci.CurrencyDecimal)return t[ze.NumberSymbols][ci.Decimal];if(n===ci.CurrencyGroup)return t[ze.NumberSymbols][ci.Group]}return i}function Kw(e){if(!e[ze.ExtraData])throw new x(2303,!1)}function Qw(e){let n=Ot(e);return Kw(n),(n[ze.ExtraData][2]||[]).map(i=>typeof i=="string"?Wm(i):[Wm(i[0]),Wm(i[1])])}function Xw(e,n,t){let i=Ot(e);Kw(i);let r=[i[ze.ExtraData][0],i[ze.ExtraData][1]],o=tn(r,n)||[];return tn(o,t)||[]}function tn(e,n){for(let t=n;t>-1;t--)if(typeof e[t]<"u")return e[t];throw new x(2304,!1)}function Wm(e){let[n,t]=e.split(":");return{hours:+n,minutes:+t}}var Ok=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Id={},Fk=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/;function Jw(e,n,t,i){let r=zk(e);n=ai(t,n)||n;let s=[],a;for(;n;)if(a=Fk.exec(n),a){s=s.concat(a.slice(1));let d=s.pop();if(!d)break;n=d}else{s.push(n);break}let c=r.getTimezoneOffset();i&&(c=tE(i,c),r=$k(r,i));let l="";return s.forEach(d=>{let u=Uk(d);l+=u?u(r,t,c):d==="''"?"'":d.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function Ad(e,n,t){let i=new Date(0);return i.setFullYear(e,n,t),i.setHours(0,0,0),i}function ai(e,n){let t=Gw(e);if(Id[t]??={},Id[t][n])return Id[t][n];let i="";switch(n){case"shortDate":i=fa(e,Rt.Short);break;case"mediumDate":i=fa(e,Rt.Medium);break;case"longDate":i=fa(e,Rt.Long);break;case"fullDate":i=fa(e,Rt.Full);break;case"shortTime":i=ha(e,Rt.Short);break;case"mediumTime":i=ha(e,Rt.Medium);break;case"longTime":i=ha(e,Rt.Long);break;case"fullTime":i=ha(e,Rt.Full);break;case"short":let r=ai(e,"shortTime"),o=ai(e,"shortDate");i=xd(pa(e,Rt.Short),[r,o]);break;case"medium":let s=ai(e,"mediumTime"),a=ai(e,"mediumDate");i=xd(pa(e,Rt.Medium),[s,a]);break;case"long":let c=ai(e,"longTime"),l=ai(e,"longDate");i=xd(pa(e,Rt.Long),[c,l]);break;case"full":let d=ai(e,"fullTime"),u=ai(e,"fullDate");i=xd(pa(e,Rt.Full),[d,u]);break}return i&&(Id[t][n]=i),i}function xd(e,n){return n&&(e=e.replace(/\{([^}]+)}/g,function(t,i){return n!=null&&i in n?n[i]:t})),e}function En(e,n,t="-",i,r){let o="";(e<0||r&&e<=0)&&(r?e=-e+1:(e=-e,o=t));let s=String(e);for(;s.length<n;)s="0"+s;return i&&(s=s.slice(s.length-n)),o+s}function Pk(e,n){return En(e,3).substring(0,n)}function Ze(e,n,t=0,i=!1,r=!1){return function(o,s){let a=Lk(e,o);if((t>0||a>-t)&&(a+=t),e===3)a===0&&t===-12&&(a=12);else if(e===6)return Pk(a,n);let c=ma(s,ci.MinusSign);return En(a,n,c,i,r)}}function Lk(e,n){switch(e){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new x(2301,!1)}}function Ae(e,n,t=yt.Format,i=!1){return function(r,o){return jk(r,o,e,n,t,i)}}function jk(e,n,t,i,r,o){switch(t){case 2:return Yw(n,r,i)[e.getMonth()];case 1:return qw(n,r,i)[e.getDay()];case 0:let s=e.getHours(),a=e.getMinutes();if(o){let l=Qw(n),d=Xw(n,r,i),u=l.findIndex(h=>{if(Array.isArray(h)){let[p,m]=h,b=s>=p.hours&&a>=p.minutes,D=s<m.hours||s===m.hours&&a<m.minutes;if(p.hours<m.hours){if(b&&D)return!0}else if(b||D)return!0}else if(h.hours===s&&h.minutes===a)return!0;return!1});if(u!==-1)return d[u]}return Ww(n,r,i)[s<12?0:1];case 3:return Zw(n,i)[e.getFullYear()<=0?0:1];default:let c=t;throw new x(2302,!1)}}function Sd(e){return function(n,t,i){let r=-1*i,o=ma(t,ci.MinusSign),s=r>0?Math.floor(r/60):Math.ceil(r/60);switch(e){case 0:return(r>=0?"+":"")+En(s,2,o)+En(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+En(s,1,o);case 2:return"GMT"+(r>=0?"+":"")+En(s,2,o)+":"+En(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+En(s,2,o)+":"+En(Math.abs(r%60),2,o);default:throw new x(2310,!1)}}}var Vk=0,Td=4;function Bk(e){let n=Ad(e,Vk,1).getDay();return Ad(e,0,1+(n<=Td?Td:Td+7)-n)}function eE(e){let n=e.getDay(),t=n===0?-3:Td-n;return Ad(e.getFullYear(),e.getMonth(),e.getDate()+t)}function qm(e,n=!1){return function(t,i){let r;if(n){let o=new Date(t.getFullYear(),t.getMonth(),1).getDay()-1,s=t.getDate();r=1+Math.floor((s+o)/7)}else{let o=eE(t),s=Bk(o.getFullYear()),a=o.getTime()-s.getTime();r=1+Math.round(a/6048e5)}return En(r,e,ma(i,ci.MinusSign))}}function Md(e,n=!1){return function(t,i){let o=eE(t).getFullYear();return En(o,e,ma(i,ci.MinusSign),n)}}var Ym={};function Uk(e){if(Ym[e])return Ym[e];let n;switch(e){case"G":case"GG":case"GGG":n=Ae(3,De.Abbreviated);break;case"GGGG":n=Ae(3,De.Wide);break;case"GGGGG":n=Ae(3,De.Narrow);break;case"y":n=Ze(0,1,0,!1,!0);break;case"yy":n=Ze(0,2,0,!0,!0);break;case"yyy":n=Ze(0,3,0,!1,!0);break;case"yyyy":n=Ze(0,4,0,!1,!0);break;case"Y":n=Md(1);break;case"YY":n=Md(2,!0);break;case"YYY":n=Md(3);break;case"YYYY":n=Md(4);break;case"M":case"L":n=Ze(1,1,1);break;case"MM":case"LL":n=Ze(1,2,1);break;case"MMM":n=Ae(2,De.Abbreviated);break;case"MMMM":n=Ae(2,De.Wide);break;case"MMMMM":n=Ae(2,De.Narrow);break;case"LLL":n=Ae(2,De.Abbreviated,yt.Standalone);break;case"LLLL":n=Ae(2,De.Wide,yt.Standalone);break;case"LLLLL":n=Ae(2,De.Narrow,yt.Standalone);break;case"w":n=qm(1);break;case"ww":n=qm(2);break;case"W":n=qm(1,!0);break;case"d":n=Ze(2,1);break;case"dd":n=Ze(2,2);break;case"c":case"cc":n=Ze(7,1);break;case"ccc":n=Ae(1,De.Abbreviated,yt.Standalone);break;case"cccc":n=Ae(1,De.Wide,yt.Standalone);break;case"ccccc":n=Ae(1,De.Narrow,yt.Standalone);break;case"cccccc":n=Ae(1,De.Short,yt.Standalone);break;case"E":case"EE":case"EEE":n=Ae(1,De.Abbreviated);break;case"EEEE":n=Ae(1,De.Wide);break;case"EEEEE":n=Ae(1,De.Narrow);break;case"EEEEEE":n=Ae(1,De.Short);break;case"a":case"aa":case"aaa":n=Ae(0,De.Abbreviated);break;case"aaaa":n=Ae(0,De.Wide);break;case"aaaaa":n=Ae(0,De.Narrow);break;case"b":case"bb":case"bbb":n=Ae(0,De.Abbreviated,yt.Standalone,!0);break;case"bbbb":n=Ae(0,De.Wide,yt.Standalone,!0);break;case"bbbbb":n=Ae(0,De.Narrow,yt.Standalone,!0);break;case"B":case"BB":case"BBB":n=Ae(0,De.Abbreviated,yt.Format,!0);break;case"BBBB":n=Ae(0,De.Wide,yt.Format,!0);break;case"BBBBB":n=Ae(0,De.Narrow,yt.Format,!0);break;case"h":n=Ze(3,1,-12);break;case"hh":n=Ze(3,2,-12);break;case"H":n=Ze(3,1);break;case"HH":n=Ze(3,2);break;case"m":n=Ze(4,1);break;case"mm":n=Ze(4,2);break;case"s":n=Ze(5,1);break;case"ss":n=Ze(5,2);break;case"S":n=Ze(6,1);break;case"SS":n=Ze(6,2);break;case"SSS":n=Ze(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Sd(0);break;case"ZZZZZ":n=Sd(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Sd(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Sd(2);break;default:return null}return Ym[e]=n,n}function tE(e,n){e=e.replace(/:/g,"");let t=Date.parse("Jan 01, 1970 00:00:00 "+e)/6e4;return isNaN(t)?n:t}function Hk(e,n){return e=new Date(e.getTime()),e.setMinutes(e.getMinutes()+n),e}function $k(e,n,t){let r=e.getTimezoneOffset(),o=tE(n,r);return Hk(e,-1*(o-r))}function zk(e){if($w(e))return e;if(typeof e=="number"&&!isNaN(e))return new Date(e);if(typeof e=="string"){if(e=e.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(e)){let[r,o=1,s=1]=e.split("-").map(a=>+a);return Ad(r,o-1,s)}let t=parseFloat(e);if(!isNaN(e-t))return new Date(t);let i;if(i=e.match(Ok))return Gk(i)}let n=new Date(e);if(!$w(n))throw new x(2311,!1);return n}function Gk(e){let n=new Date(0),t=0,i=0,r=e[8]?n.setUTCFullYear:n.setFullYear,o=e[8]?n.setUTCHours:n.setHours;e[9]&&(t=Number(e[9]+e[10]),i=Number(e[9]+e[11])),r.call(n,Number(e[1]),Number(e[2])-1,Number(e[3]));let s=Number(e[4]||0)-t,a=Number(e[5]||0)-i,c=Number(e[6]||0),l=Math.floor(parseFloat("0."+(e[7]||0))*1e3);return o.call(n,s,a,c,l),n}function $w(e){return e instanceof Date&&!isNaN(e.valueOf())}var Zm=/\s+/,zw=[],Xm=(()=>{class e{_ngEl;_renderer;initialClasses=zw;rawClass;stateMap=new Map;constructor(t,i){this._ngEl=t,this._renderer=i}set klass(t){this.initialClasses=t!=null?t.trim().split(Zm):zw}set ngClass(t){this.rawClass=typeof t=="string"?t.trim().split(Zm):t}ngDoCheck(){for(let i of this.initialClasses)this._updateState(i,!0);let t=this.rawClass;if(Array.isArray(t)||t instanceof Set)for(let i of t)this._updateState(i,!0);else if(t!=null)for(let i of Object.keys(t))this._updateState(i,!!t[i]);this._applyStateDiff()}_updateState(t,i){let r=this.stateMap.get(t);r!==void 0?(r.enabled!==i&&(r.changed=!0,r.enabled=i),r.touched=!0):this.stateMap.set(t,{enabled:i,changed:!0,touched:!0})}_applyStateDiff(){for(let t of this.stateMap){let i=t[0],r=t[1];r.changed?(this._toggleClass(i,r.enabled),r.changed=!1):r.touched||(r.enabled&&this._toggleClass(i,!1),this.stateMap.delete(i)),r.touched=!1}}_toggleClass(t,i){t=t.trim(),t.length>0&&t.split(Zm).forEach(r=>{i?this._renderer.addClass(this._ngEl.nativeElement,r):this._renderer.removeClass(this._ngEl.nativeElement,r)})}static \u0275fac=function(i){return new(i||e)(ee(Q),ee(it))};static \u0275dir=B({type:e,selectors:[["","ngClass",""]],inputs:{klass:[0,"class","klass"],ngClass:"ngClass"}})}return e})();var Jm=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=f(re);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(t,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||e)(ee(yn))};static \u0275dir=B({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[xt]})}return e})();function nE(e,n){return new x(2100,!1)}var Km=class{createSubscription(n,t,i){return je(()=>n.subscribe({next:t,error:i}))}dispose(n){je(()=>n.unsubscribe())}},Qm=class{createSubscription(n,t,i){return n.then(r=>t?.(r),r=>i?.(r)),{unsubscribe:()=>{t=null,i=null}}}dispose(n){n.unsubscribe()}},Wk=new Qm,qk=new Km,eg=(()=>{class e{_ref;_latestValue=null;markForCheckOnValueUpdate=!0;_subscription=null;_obj=null;_strategy=null;applicationErrorHandler=f(Wt);constructor(t){this._ref=t}ngOnDestroy(){this._subscription&&this._dispose(),this._ref=null}transform(t){if(!this._obj){if(t)try{this.markForCheckOnValueUpdate=!1,this._subscribe(t)}finally{this.markForCheckOnValueUpdate=!0}return this._latestValue}return t!==this._obj?(this._dispose(),this.transform(t)):this._latestValue}_subscribe(t){this._obj=t,this._strategy=this._selectStrategy(t),this._subscription=this._strategy.createSubscription(t,i=>this._updateLatestValue(t,i),i=>this.applicationErrorHandler(i))}_selectStrategy(t){if(oi(t))return Wk;if(pd(t))return qk;throw nE(e,t)}_dispose(){this._strategy.dispose(this._subscription),this._latestValue=null,this._subscription=null,this._obj=null}_updateLatestValue(t,i){t===this._obj&&(this._latestValue=i,this.markForCheckOnValueUpdate&&this._ref?.markForCheck())}static \u0275fac=function(i){return new(i||e)(ee(ht,16))};static \u0275pipe=na({name:"async",type:e,pure:!1})}return e})();var Yk="mediumDate",iE=new y(""),rE=new y(""),ga=(()=>{class e{locale;defaultTimezone;defaultOptions;constructor(t,i,r){this.locale=t,this.defaultTimezone=i,this.defaultOptions=r}transform(t,i,r,o){if(t==null||t===""||t!==t)return null;try{let s=i??this.defaultOptions?.dateFormat??Yk,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return Jw(t,s,o||this.locale,a)}catch(s){throw nE(e,s.message)}}static \u0275fac=function(i){return new(i||e)(ee(ca,16),ee(iE,24),ee(rE,24))};static \u0275pipe=na({name:"date",type:e,pure:!0})}return e})();var tg=(()=>{class e{transform(t){return JSON.stringify(t,null,2)}static \u0275fac=function(i){return new(i||e)};static \u0275pipe=na({name:"json",type:e,pure:!1})}return e})();var Io=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({})}return e})();function va(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let i=t.indexOf("="),[r,o]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Sr=class{};var ng="browser";function oE(e){return e===ng}var ya=class{_doc;constructor(n){this._doc=n}manager},Rd=(()=>{class e extends ya{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,o){return t.addEventListener(i,r,o),()=>this.removeEventListener(t,i,r,o)}removeEventListener(t,i,r,o){return t.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||e)(I(W))};static \u0275prov=E({token:e,factory:e.\u0275fac})}return e})(),Od=new y(""),sg=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(s=>{s.manager=this});let r=t.filter(s=>!(s instanceof Rd));this._plugins=r.slice().reverse();let o=t.find(s=>s instanceof Rd);o&&this._plugins.push(o)}addEventListener(t,i,r,o){return this._findPluginFor(i).addEventListener(t,i,r,o)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(o=>o.supports(t)),!i)throw new x(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||e)(I(Od),I(j))};static \u0275prov=E({token:e,factory:e.\u0275fac})}return e})(),ig="ng-app-id";function aE(e){for(let n of e)n.remove()}function cE(e,n){let t=n.createElement("style");return t.textContent=e,t}function Kk(e,n,t,i){let r=e.head?.querySelectorAll(`style[${ig}="${n}"],link[${ig}="${n}"]`);if(r)for(let o of r)o.removeAttribute(ig),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]})}function og(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var ag=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,o={}){this.doc=t,this.appId=i,this.nonce=r,Kk(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,cE);i?.forEach(r=>this.addUsage(r,this.external,og))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let o=i.get(t);o?o.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(aE(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])aE(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,cE(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,og(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||e)(I(W),I(wo),I(Dr,8),I(Cr))};static \u0275prov=E({token:e,factory:e.\u0275fac})}return e})(),rg={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},cg=/%COMP%/g;var dE="%COMP%",Qk=`_nghost-${dE}`,Xk=`_ngcontent-${dE}`,Jk=!0,eN=new y("",{factory:()=>Jk});function tN(e){return Xk.replace(cg,e)}function nN(e){return Qk.replace(cg,e)}function uE(e,n){return n.map(t=>t.replace(cg,e))}var lg=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new _a(t,s,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Nd?r.applyToHost(t):r instanceof ba&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case gn.Emulated:o=new Nd(c,l,i,this.appId,d,s,a,u);break;case gn.ShadowDom:return new kd(c,t,i,s,a,this.nonce,u,l);case gn.ExperimentalIsolatedShadowDom:return new kd(c,t,i,s,a,this.nonce,u);default:o=new ba(c,l,i,d,s,a,u);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||e)(I(sg),I(ag),I(wo),I(eN),I(W),I(j),I(Dr),I(vn,8))};static \u0275prov=E({token:e,factory:e.\u0275fac})}return e})(),_a=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,i,r){this.eventManager=n,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(rg[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){(lE(n)?n.content:n).appendChild(t)}insertBefore(n,t,i){n&&(lE(n)?n.content:n).insertBefore(t,i)}removeChild(n,t){t.remove()}selectRootElement(n,t){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new x(-5104,!1);return t||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,i,r){if(r){t=r+":"+t;let o=rg[r];o?n.setAttributeNS(o,t,i):n.setAttribute(t,i)}else n.setAttribute(t,i)}removeAttribute(n,t,i){if(i){let r=rg[i];r?n.removeAttributeNS(r,t):n.removeAttribute(`${i}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,i,r){r&(Nn.DashCase|Nn.Important)?n.style.setProperty(t,i,r&Nn.Important?"important":""):n.style[t]=i}removeStyle(n,t,i){i&Nn.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,i){n!=null&&(n[t]=i)}setValue(n,t){n.nodeValue=t}listen(n,t,i,r){if(typeof n=="string"&&(n=en().getGlobalEventTarget(this.doc,n),!n))throw new x(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,t,o)),this.eventManager.addEventListener(n,t,o,r)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function lE(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var kd=class extends _a{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=uE(i.id,l);for(let u of l){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=u,this.shadowRoot.appendChild(h)}let d=i.getExternalStyles?.();if(d)for(let u of d){let h=og(u,r);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,i){return super.insertBefore(this.nodeOrShadowRoot(n),t,i)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ba=class extends _a{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?uE(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&br.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Nd=class extends ba{contentAttr;hostAttr;constructor(n,t,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,t,i,o,s,a,c,l),this.contentAttr=tN(l),this.hostAttr=nN(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let i=super.createElement(n,t);return super.setAttribute(i,this.contentAttr,""),i}};var Fd=class e extends ua{supportsDOMEvents=!0;static makeCurrent(){Gm(new e)}onAndCancel(n,t,i,r){return n.addEventListener(t,i,r),()=>{n.removeEventListener(t,i,r)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=iN();return t==null?null:rN(t)}resetBaseElement(){wa=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return va(document.cookie,n)}},wa=null;function iN(){return wa=wa||document.head.querySelector("base"),wa?wa.getAttribute("href"):null}function rN(e){return new URL(e,document.baseURI).pathname}var oN=(()=>{class e{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac})}return e})(),fE=["alt","control","meta","shift"],sN={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},aN={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},hE=(()=>{class e extends ya{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,i,r,o){let s=e.parseEventName(i),a=e.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>en().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=e._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),fE.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(t,i){let r=sN[t.key]||t.key,o="";return i.indexOf("code.")>-1&&(r=t.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),fE.forEach(s=>{if(s!==r){let a=aN[s];a(t)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(t,i,r){return o=>{e.matchEventFullKeyCode(o,t)&&r.runGuarded(()=>i(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||e)(I(W))};static \u0275prov=E({token:e,factory:e.\u0275fac})}return e})();function dg(e,n,t){return Se(this,null,function*(){let i=_({rootComponent:e},cN(n,t));return Fw(i)})}function cN(e,n){return{platformRef:n?.platformRef,appProviders:[...hN,...e?.providers??[]],platformProviders:fN}}function lN(){Fd.makeCurrent()}function dN(){return new Ye}function uN(){return am(document),document}var fN=[{provide:Cr,useValue:ng},{provide:td,useValue:lN,multi:!0},{provide:W,useFactory:uN}];var hN=[{provide:As,useValue:"root"},{provide:Ye,useFactory:dN},{provide:Od,useClass:Rd,multi:!0},{provide:Od,useClass:hE,multi:!0},lg,ag,sg,{provide:ut,useExisting:lg},{provide:Sr,useClass:oN},[]];var Bi=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),o=t.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let t=this.headers.get(n.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,t){return this.clone({name:n,value:t,op:"a"})}set(n,t){return this.clone({name:n,value:t,op:"s"})}delete(n,t){return this.clone({name:n,value:t,op:"d"})}maybeSetNormalizedName(n,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,n)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(t=>{this.headers.set(t,n.headers.get(t)),this.normalizedNames.set(t,n.normalizedNames.get(t))})}clone(n){let t=new e;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([n]),t}applyUpdate(n){let t=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,t);let r=(n.op==="a"?this.headers.get(t):void 0)||[];r.push(...i),this.headers.set(t,r);break;case"d":let o=n.value;if(!o)this.headers.delete(t),this.normalizedNames.delete(t);else{let s=this.headers.get(t);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,s)}break}}addHeaderEntry(n,t){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(n,t){let i=(Array.isArray(t)?t:[t]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>n(this.normalizedNames.get(t),this.headers.get(t)))}};var Ld=class{map=new Map;set(n,t){return this.map.set(n,t),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},jd=class{encodeKey(n){return pE(n)}encodeValue(n){return pE(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function pN(e,n){let t=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=t.get(s)||[];c.push(a),t.set(s,c)}),t}var mN=/%(\d[a-f0-9])/gi,gN={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function pE(e){return encodeURIComponent(e).replace(mN,(n,t)=>gN[t]??n)}function Pd(e){return`${e}`}var li=class e{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new jd,n.fromString){if(n.fromObject)throw new x(2805,!1);this.map=pN(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(t=>{let i=n.fromObject[t],r=Array.isArray(i)?i.map(Pd):[Pd(i)];this.map.set(t,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let t=this.map.get(n);return t?t[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,t){return this.clone({param:n,value:t,op:"a"})}appendAll(n){let t=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{t.push({param:i,value:o,op:"a"})}):t.push({param:i,value:r,op:"a"})}),this.clone(t)}set(n,t){return this.clone({param:n,value:t,op:"s"})}delete(n,t){return this.clone({param:n,value:t,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let t=this.encoder.encodeKey(n);return this.map.get(n).map(i=>t+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let t=new e({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(n),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let t=(n.op==="a"?this.map.get(n.param):void 0)||[];t.push(Pd(n.value)),this.map.set(n.param,t);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Pd(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function vN(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function mE(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function gE(e){return typeof Blob<"u"&&e instanceof Blob}function vE(e){return typeof FormData<"u"&&e instanceof FormData}function yN(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var yE="Content-Type",_E="Accept",bE="text/plain",wE="application/json",_N=`${wE}, ${bE}, */*`,xo=class e{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,t,i,r){this.url=t,this.method=n.toUpperCase();let o;if(vN(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new x(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Bi,this.context??=new Ld,!this.params)this.params=new li,this.urlWithParams=t;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=t;else{let a=t.indexOf("?"),c=a===-1?"?":a<t.length-1?"&":"";this.urlWithParams=t+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||mE(this.body)||gE(this.body)||vE(this.body)||yN(this.body)?this.body:this.body instanceof li?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||vE(this.body)?null:gE(this.body)?this.body.type||null:mE(this.body)?null:typeof this.body=="string"?bE:this.body instanceof li?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?wE:null}clone(n={}){let t=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,u=n.referrer||this.referrer,h=n.integrity||this.integrity,p=n.referrerPolicy||this.referrerPolicy,m=n.transferCache??this.transferCache,b=n.timeout??this.timeout,D=n.body!==void 0?n.body:this.body,S=n.withCredentials??this.withCredentials,L=n.reportProgress??this.reportProgress,ue=n.headers||this.headers,G=n.params||this.params,fe=n.context??this.context;return n.setHeaders!==void 0&&(ue=Object.keys(n.setHeaders).reduce((he,ne)=>he.set(ne,n.setHeaders[ne]),ue)),n.setParams&&(G=Object.keys(n.setParams).reduce((he,ne)=>he.set(ne,n.setParams[ne]),G)),new e(t,i,D,{params:G,headers:ue,context:fe,reportProgress:L,responseType:r,withCredentials:S,transferCache:m,keepalive:o,cache:a,priority:s,timeout:b,mode:c,redirect:l,credentials:d,referrer:u,integrity:h,referrerPolicy:p})}},Mr=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(Mr||{}),Mo=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,t=200,i="OK"){this.headers=n.headers||new Bi,this.status=n.status!==void 0?n.status:t,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Vd=class e extends Mo{constructor(n={}){super(n)}type=Mr.ResponseHeader;clone(n={}){return new e({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Ea=class e extends Mo{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Mr.Response;clone(n={}){return new e({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},So=class extends Mo{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},bN=200,wN=204;var EN=new y("");var CN=/^\)\]\}',?\n/;var fg=(()=>{class e{xhrFactory;tracingService=f(vn,{optional:!0});constructor(t){this.xhrFactory=t}maybePropagateTrace(t){return this.tracingService?.propagate?this.tracingService.propagate(t):t}handle(t){if(t.method==="JSONP")throw new x(-2800,!1);let i=this.xhrFactory;return R(null).pipe(He(()=>new H(o=>{let s=i.build();if(s.open(t.method,t.urlWithParams),t.withCredentials&&(s.withCredentials=!0),t.headers.forEach((D,S)=>s.setRequestHeader(D,S.join(","))),t.headers.has(_E)||s.setRequestHeader(_E,_N),!t.headers.has(yE)){let D=t.detectContentTypeHeader();D!==null&&s.setRequestHeader(yE,D)}if(t.timeout&&(s.timeout=t.timeout),t.responseType){let D=t.responseType.toLowerCase();s.responseType=D!=="json"?D:"text"}let a=t.serializeBody(),c=null,l=()=>{if(c!==null)return c;let D=s.statusText||"OK",S=new Bi(s.getAllResponseHeaders()),L=s.responseURL||t.url;return c=new Vd({headers:S,status:s.status,statusText:D,url:L}),c},d=this.maybePropagateTrace(()=>{let{headers:D,status:S,statusText:L,url:ue}=l(),G=null;S!==wN&&(G=typeof s.response>"u"?s.responseText:s.response),S===0&&(S=G?bN:0);let fe=S>=200&&S<300;if(t.responseType==="json"&&typeof G=="string"){let he=G;G=G.replace(CN,"");try{G=G!==""?JSON.parse(G):null}catch(ne){G=he,fe&&(fe=!1,G={error:ne,text:G})}}fe?(o.next(new Ea({body:G,headers:D,status:S,statusText:L,url:ue||void 0})),o.complete()):o.error(new So({error:G,headers:D,status:S,statusText:L,url:ue||void 0}))}),u=this.maybePropagateTrace(D=>{let{url:S}=l(),L=new So({error:D,status:s.status||0,statusText:s.statusText||"Unknown Error",url:S||void 0});o.error(L)}),h=u;t.timeout&&(h=this.maybePropagateTrace(D=>{let{url:S}=l(),L=new So({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:S||void 0});o.error(L)}));let p=!1,m=this.maybePropagateTrace(D=>{p||(o.next(l()),p=!0);let S={type:Mr.DownloadProgress,loaded:D.loaded};D.lengthComputable&&(S.total=D.total),t.responseType==="text"&&s.responseText&&(S.partialText=s.responseText),o.next(S)}),b=this.maybePropagateTrace(D=>{let S={type:Mr.UploadProgress,loaded:D.loaded};D.lengthComputable&&(S.total=D.total),o.next(S)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",h),s.addEventListener("abort",u),t.reportProgress&&(s.addEventListener("progress",m),a!==null&&s.upload&&s.upload.addEventListener("progress",b)),s.send(a),o.next({type:Mr.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",h),t.reportProgress&&(s.removeEventListener("progress",m),a!==null&&s.upload&&s.upload.removeEventListener("progress",b)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||e)(I(Sr))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function DN(e,n){return n(e)}function IN(e,n,t){return(i,r)=>et(t,()=>n(i,o=>e(o,r)))}var EE=new y("",{factory:()=>[]}),CE=new y(""),DE=new y("",{factory:()=>!0});var hg=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=I(fg),r},providedIn:"root"})}return e})();var Bd=(()=>{class e{backend;injector;chain=null;pendingTasks=f(fo);contributeToStability=f(DE);constructor(t,i){this.backend=t,this.injector=i}handle(t){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(EE),...this.injector.get(CE,[])]));this.chain=i.reduceRight((r,o)=>IN(r,o,this.injector),DN)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(t,r=>this.backend.handle(r)).pipe(ir(i))}else return this.chain(t,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||e)(I(hg),I(Ee))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),pg=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=I(Bd),r},providedIn:"root"})}return e})();function ug(e,n){return{body:n,headers:e.headers,context:e.context,observe:e.observe,params:e.params,reportProgress:e.reportProgress,responseType:e.responseType,withCredentials:e.withCredentials,credentials:e.credentials,transferCache:e.transferCache,timeout:e.timeout,keepalive:e.keepalive,priority:e.priority,cache:e.cache,mode:e.mode,redirect:e.redirect,integrity:e.integrity,referrer:e.referrer,referrerPolicy:e.referrerPolicy}}var To=(()=>{class e{handler;constructor(t){this.handler=t}request(t,i,r={}){let o;if(t instanceof xo)o=t;else{let c;r.headers instanceof Bi?c=r.headers:c=new Bi(r.headers);let l;r.params&&(r.params instanceof li?l=r.params:l=new li({fromObject:r.params})),o=new xo(t,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=R(o).pipe(wi(c=>this.handler.handle(c)));if(t instanceof xo||r.observe==="events")return s;let a=s.pipe(_e(c=>c instanceof Ea));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(T(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new x(2806,!1);return c.body}));case"blob":return a.pipe(T(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new x(2807,!1);return c.body}));case"text":return a.pipe(T(c=>{if(c.body!==null&&typeof c.body!="string")throw new x(2808,!1);return c.body}));default:return a.pipe(T(c=>c.body))}case"response":return a;default:throw new x(2809,!1)}}delete(t,i={}){return this.request("DELETE",t,i)}get(t,i={}){return this.request("GET",t,i)}head(t,i={}){return this.request("HEAD",t,i)}jsonp(t,i){return this.request("JSONP",t,{params:new li().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,i={}){return this.request("OPTIONS",t,i)}patch(t,i,r={}){return this.request("PATCH",t,ug(r,i))}post(t,i,r={}){return this.request("POST",t,ug(r,i))}put(t,i,r={}){return this.request("PUT",t,ug(r,i))}static \u0275fac=function(i){return new(i||e)(I(pg))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var xN=new y("",{factory:()=>!0}),SN="XSRF-TOKEN",MN=new y("",{factory:()=>SN}),TN="X-XSRF-TOKEN",AN=new y("",{factory:()=>TN}),RN=(()=>{class e{cookieName=f(MN);doc=f(W);lastCookieString="";lastToken=null;parseCount=0;getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=va(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),IE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=I(RN),r},providedIn:"root"})}return e})();function kN(e,n){if(!f(xN)||e.method==="GET"||e.method==="HEAD")return n(e);try{let r=f(Do).href,{origin:o}=new URL(r),{origin:s}=new URL(e.url,o);if(o!==s)return n(e)}catch(r){return n(e)}let t=f(IE).getToken(),i=f(AN);return t!=null&&!e.headers.has(i)&&(e=e.clone({headers:e.headers.set(i,t)})),n(e)}function mg(...e){let n=[To,Bd,{provide:pg,useExisting:Bd},{provide:hg,useFactory:()=>f(EN,{optional:!0})??f(fg)},{provide:EE,useValue:kN,multi:!0}];for(let t of e)n.push(...t.\u0275providers);return Tt(n)}var SE=(()=>{class e{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||e)(I(W))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Ca=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=I(NN),r},providedIn:"root"})}return e})(),NN=(()=>{class e extends Ca{_doc;constructor(t){super(),this._doc=t}sanitize(t,i){if(i==null)return null;switch(t){case nt.NONE:return i;case nt.HTML:return Oi(i,"HTML")?Pn(i):hm(this._doc,String(i)).toString();case nt.STYLE:return Oi(i,"Style")?Pn(i):i;case nt.SCRIPT:if(Oi(i,"Script"))return Pn(i);throw new x(5200,!1);case nt.URL:return Oi(i,"URL")?Pn(i):Ks(String(i));case nt.RESOURCE_URL:if(Oi(i,"ResourceURL"))return Pn(i);throw new x(5201,!1);default:throw new x(5202,!1)}}bypassSecurityTrustHtml(t){return cm(t)}bypassSecurityTrustStyle(t){return lm(t)}bypassSecurityTrustScript(t){return dm(t)}bypassSecurityTrustUrl(t){return um(t)}bypassSecurityTrustResourceUrl(t){return fm(t)}static \u0275fac=function(i){return new(i||e)(I(W))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var J="primary",La=Symbol("RouteTitle"),bg=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t[0]:t}return null}getAll(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function Ar(e){return new bg(e)}function gg(e,n,t){for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(r[0]===":")t[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function PE(e,n,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>e.length||t.pathMatch==="full"&&(n.hasChildren()||i.length<e.length))return null;let c={},l=e.slice(0,i.length);return gg(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>e.length||t.pathMatch==="full"&&n.hasChildren()&&t.path!=="**")return null;let a={};return!gg(o,e.slice(0,o.length),a)||!gg(s,e.slice(e.length-s.length),a)?null:{consumed:e,posParams:a}}function Wd(e){return new Promise((n,t)=>{e.pipe(qn()).subscribe({next:i=>n(i),error:i=>t(i)})})}function ON(e,n){if(e.length!==n.length)return!1;for(let t=0;t<e.length;++t)if(!Bn(e[t],n[t]))return!1;return!0}function Bn(e,n){let t=e?wg(e):void 0,i=n?wg(n):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let o=0;o<t.length;o++)if(r=t[o],!LE(e[r],n[r]))return!1;return!0}function wg(e){return[...Object.keys(e),...Object.getOwnPropertySymbols(e)]}function LE(e,n){if(Array.isArray(e)&&Array.isArray(n)){if(e.length!==n.length)return!1;let t=[...e].sort(),i=[...n].sort();return t.every((r,o)=>i[o]===r)}else return e===n}function FN(e){return e.length>0?e[e.length-1]:null}function Nr(e){return qc(e)?e:oi(e)?ke(Promise.resolve(e)):R(e)}function jE(e){return qc(e)?Wd(e):Promise.resolve(e)}var PN={exact:UE,subset:HE},VE={exact:LN,subset:jN,ignored:()=>!0},BE={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Eg={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function TE(e,n,t){return PN[t.paths](e.root,n.root,t.matrixParams)&&VE[t.queryParams](e.queryParams,n.queryParams)&&!(t.fragment==="exact"&&e.fragment!==n.fragment)}function LN(e,n){return Bn(e,n)}function UE(e,n,t){if(!Tr(e.segments,n.segments)||!$d(e.segments,n.segments,t)||e.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!e.children[i]||!UE(e.children[i],n.children[i],t))return!1;return!0}function jN(e,n){return Object.keys(n).length<=Object.keys(e).length&&Object.keys(n).every(t=>LE(e[t],n[t]))}function HE(e,n,t){return $E(e,n,n.segments,t)}function $E(e,n,t,i){if(e.segments.length>t.length){let r=e.segments.slice(0,t.length);return!(!Tr(r,t)||n.hasChildren()||!$d(r,t,i))}else if(e.segments.length===t.length){if(!Tr(e.segments,t)||!$d(e.segments,t,i))return!1;for(let r in n.children)if(!e.children[r]||!HE(e.children[r],n.children[r],i))return!1;return!0}else{let r=t.slice(0,e.segments.length),o=t.slice(e.segments.length);return!Tr(e.segments,r)||!$d(e.segments,r,i)||!e.children[J]?!1:$E(e.children[J],n,o,i)}}function $d(e,n,t){return n.every((i,r)=>VE[t](e[r].parameters,i.parameters))}var rn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new me([],{}),t={},i=null){this.root=n,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Ar(this.queryParams),this._queryParamMap}toString(){return UN.serialize(this)}},me=class{segments;children;parent=null;constructor(n,t){this.segments=n,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return zd(this)}},Ui=class{path;parameters;_parameterMap;constructor(n,t){this.path=n,this.parameters=t}get parameterMap(){return this._parameterMap??=Ar(this.parameters),this._parameterMap}toString(){return GE(this)}};function VN(e,n){return Tr(e,n)&&e.every((t,i)=>Bn(t.parameters,n[i].parameters))}function Tr(e,n){return e.length!==n.length?!1:e.every((t,i)=>t.path===n[i].path)}function BN(e,n){let t=[];return Object.entries(e.children).forEach(([i,r])=>{i===J&&(t=t.concat(n(r,i)))}),Object.entries(e.children).forEach(([i,r])=>{i!==J&&(t=t.concat(n(r,i)))}),t}var ja=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>new Hi,providedIn:"root"})}return e})(),Hi=class{parse(n){let t=new Dg(n);return new rn(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(n){let t=`/${Da(n.root,!0)}`,i=zN(n.queryParams),r=typeof n.fragment=="string"?`#${HN(n.fragment)}`:"";return`${t}${i}${r}`}},UN=new Hi;function zd(e){return e.segments.map(n=>GE(n)).join("/")}function Da(e,n){if(!e.hasChildren())return zd(e);if(n){let t=e.children[J]?Da(e.children[J],!1):"",i=[];return Object.entries(e.children).forEach(([r,o])=>{r!==J&&i.push(`${r}:${Da(o,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=BN(e,(i,r)=>r===J?[Da(e.children[J],!1)]:[`${r}:${Da(i,!1)}`]);return Object.keys(e.children).length===1&&e.children[J]!=null?`${zd(e)}/${t[0]}`:`${zd(e)}/(${t.join("//")})`}}function zE(e){return encodeURIComponent(e).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Ud(e){return zE(e).replace(/%3B/gi,";")}function HN(e){return encodeURI(e)}function Cg(e){return zE(e).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Gd(e){return decodeURIComponent(e)}function AE(e){return Gd(e.replace(/\+/g,"%20"))}function GE(e){return`${Cg(e.path)}${$N(e.parameters)}`}function $N(e){return Object.entries(e).map(([n,t])=>`;${Cg(n)}=${Cg(t)}`).join("")}function zN(e){let n=Object.entries(e).map(([t,i])=>Array.isArray(i)?i.map(r=>`${Ud(t)}=${Ud(r)}`).join("&"):`${Ud(t)}=${Ud(i)}`).filter(t=>t);return n.length?`?${n.join("&")}`:""}var GN=/^[^\/()?;#]+/;function vg(e){let n=e.match(GN);return n?n[0]:""}var WN=/^[^\/()?;=#]+/;function qN(e){let n=e.match(WN);return n?n[0]:""}var YN=/^[^=?&#]+/;function ZN(e){let n=e.match(YN);return n?n[0]:""}var KN=/^[^&#]+/;function QN(e){let n=e.match(KN);return n?n[0]:""}var Dg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new me([],{}):new me([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new x(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(t.length>0||Object.keys(i).length>0)&&(r[J]=new me(t,i)),r}parseSegment(){let n=vg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new x(4009,!1);return this.capture(n),new Ui(Gd(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let t=qN(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=vg(this.remaining);r&&(i=r,this.capture(i))}n[Gd(t)]=Gd(i)}parseQueryParam(n){let t=ZN(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let s=QN(this.remaining);s&&(i=s,this.capture(i))}let r=AE(t),o=AE(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=vg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new x(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=J);let a=this.parseChildren(t+1);i[s??J]=Object.keys(a).length===1&&a[J]?a[J]:new me([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new x(4011,!1)}};function WE(e){return e.segments.length>0?new me([],{[J]:e}):e}function qE(e){let n={};for(let[i,r]of Object.entries(e.children)){let o=qE(r);if(i===J&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let t=new me(e.segments,n);return XN(t)}function XN(e){if(e.numberOfChildren===1&&e.children[J]){let n=e.children[J];return new me(e.segments.concat(n.segments),n.children)}return e}function No(e){return e instanceof rn}function YE(e,n,t=null,i=null,r=new Hi){let o=ZE(e);return KE(o,n,t,i,r)}function ZE(e){let n;function t(o){let s={};for(let c of o.children){let l=t(c);s[c.outlet]=l}let a=new me(o.url,s);return o===e&&(n=a),a}let i=t(e.root),r=WE(i);return n??r}function KE(e,n,t,i,r){let o=e;for(;o.parent;)o=o.parent;if(n.length===0)return yg(o,o,o,t,i,r);let s=JN(n);if(s.toRoot())return yg(o,o,new me([],{}),t,i,r);let a=eO(s,o,e),c=a.processChildren?xa(a.segmentGroup,a.index,s.commands):XE(a.segmentGroup,a.index,s.commands);return yg(o,a.segmentGroup,c,t,i,r)}function qd(e){return typeof e=="object"&&e!=null&&!e.outlets&&!e.segmentPath}function Ta(e){return typeof e=="object"&&e!=null&&e.outlets}function RE(e,n,t){e||="\u0275";let i=new rn;return i.queryParams={[e]:n},t.parse(t.serialize(i)).queryParams[e]}function yg(e,n,t,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(u=>RE(l,u,o)):RE(l,d,o);let a;e===n?a=t:a=QE(e,n,t);let c=WE(qE(a));return new rn(c,s,r)}function QE(e,n,t){let i={};return Object.entries(e.children).forEach(([r,o])=>{o===n?i[r]=t:i[r]=QE(o,n,t)}),new me(e.segments,i)}var Yd=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,t,i){if(this.isAbsolute=n,this.numberOfDoubleDots=t,this.commands=i,n&&i.length>0&&qd(i[0]))throw new x(4003,!1);let r=i.find(Ta);if(r&&r!==FN(i))throw new x(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function JN(e){if(typeof e[0]=="string"&&e.length===1&&e[0]==="/")return new Yd(!0,0,e);let n=0,t=!1,i=e.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Yd(t,n,i)}var Ro=class{segmentGroup;processChildren;index;constructor(n,t,i){this.segmentGroup=n,this.processChildren=t,this.index=i}};function eO(e,n,t){if(e.isAbsolute)return new Ro(n,!0,0);if(!t)return new Ro(n,!1,NaN);if(t.parent===null)return new Ro(t,!0,0);let i=qd(e.commands[0])?0:1,r=t.segments.length-1+i;return tO(t,r,e.numberOfDoubleDots)}function tO(e,n,t){let i=e,r=n,o=t;for(;o>r;){if(o-=r,i=i.parent,!i)throw new x(4005,!1);r=i.segments.length}return new Ro(i,!1,r-o)}function nO(e){return Ta(e[0])?e[0].outlets:{[J]:e}}function XE(e,n,t){if(e??=new me([],{}),e.segments.length===0&&e.hasChildren())return xa(e,n,t);let i=iO(e,n,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<e.segments.length){let o=new me(e.segments.slice(0,i.pathIndex),{});return o.children[J]=new me(e.segments.slice(i.pathIndex),e.children),xa(o,0,r)}else return i.match&&r.length===0?new me(e.segments,{}):i.match&&!e.hasChildren()?Ig(e,n,t):i.match?xa(e,0,r):Ig(e,n,t)}function xa(e,n,t){if(t.length===0)return new me(e.segments,{});{let i=nO(t),r={};if(Object.keys(i).some(o=>o!==J)&&e.children[J]&&e.numberOfChildren===1&&e.children[J].segments.length===0){let o=xa(e.children[J],n,t);return new me(e.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=XE(e.children[o],n,s))}),Object.entries(e.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new me(e.segments,r)}}function iO(e,n,t){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<e.segments.length;){if(i>=t.length)return o;let s=e.segments[r],a=t[i];if(Ta(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!NE(c,l,s))return o;i+=2}else{if(!NE(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Ig(e,n,t){let i=e.segments.slice(0,n),r=0;for(;r<t.length;){let o=t[r];if(Ta(o)){let c=rO(o.outlets);return new me(i,c)}if(r===0&&qd(t[0])){let c=e.segments[n];i.push(new Ui(c.path,kE(t[0]))),r++;continue}let s=Ta(o)?o.outlets[J]:`${o}`,a=r<t.length-1?t[r+1]:null;s&&a&&qd(a)?(i.push(new Ui(s,kE(a))),r+=2):(i.push(new Ui(s,{})),r++)}return new me(i,{})}function rO(e){let n={};return Object.entries(e).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[t]=Ig(new me([],{}),0,i))}),n}function kE(e){let n={};return Object.entries(e).forEach(([t,i])=>n[t]=`${i}`),n}function NE(e,n,t){return e==t.path&&Bn(n,t.parameters)}var Sa="imperative",at=(function(e){return e[e.NavigationStart=0]="NavigationStart",e[e.NavigationEnd=1]="NavigationEnd",e[e.NavigationCancel=2]="NavigationCancel",e[e.NavigationError=3]="NavigationError",e[e.RoutesRecognized=4]="RoutesRecognized",e[e.ResolveStart=5]="ResolveStart",e[e.ResolveEnd=6]="ResolveEnd",e[e.GuardsCheckStart=7]="GuardsCheckStart",e[e.GuardsCheckEnd=8]="GuardsCheckEnd",e[e.RouteConfigLoadStart=9]="RouteConfigLoadStart",e[e.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",e[e.ChildActivationStart=11]="ChildActivationStart",e[e.ChildActivationEnd=12]="ChildActivationEnd",e[e.ActivationStart=13]="ActivationStart",e[e.ActivationEnd=14]="ActivationEnd",e[e.Scroll=15]="Scroll",e[e.NavigationSkipped=16]="NavigationSkipped",e})(at||{}),Pt=class{id;url;constructor(n,t){this.id=n,this.url=t}},Rr=class extends Pt{type=at.NavigationStart;navigationTrigger;restoredState;constructor(n,t,i="imperative",r=null){super(n,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ui=class extends Pt{urlAfterRedirects;type=at.NavigationEnd;constructor(n,t,i){super(n,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},_t=(function(e){return e[e.Redirect=0]="Redirect",e[e.SupersededByNewNavigation=1]="SupersededByNewNavigation",e[e.NoDataFromResolver=2]="NoDataFromResolver",e[e.GuardRejected=3]="GuardRejected",e[e.Aborted=4]="Aborted",e})(_t||{}),Aa=(function(e){return e[e.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",e[e.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",e})(Aa||{}),nn=class extends Pt{reason;code;type=at.NavigationCancel;constructor(n,t,i,r){super(n,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function JE(e){return e instanceof nn&&(e.code===_t.Redirect||e.code===_t.SupersededByNewNavigation)}var fi=class extends Pt{reason;code;type=at.NavigationSkipped;constructor(n,t,i,r){super(n,t),this.reason=i,this.code=r}},kr=class extends Pt{error;target;type=at.NavigationError;constructor(n,t,i,r){super(n,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Ra=class extends Pt{urlAfterRedirects;state;type=at.RoutesRecognized;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Zd=class extends Pt{urlAfterRedirects;state;type=at.GuardsCheckStart;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Kd=class extends Pt{urlAfterRedirects;state;shouldActivate;type=at.GuardsCheckEnd;constructor(n,t,i,r,o){super(n,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Qd=class extends Pt{urlAfterRedirects;state;type=at.ResolveStart;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Xd=class extends Pt{urlAfterRedirects;state;type=at.ResolveEnd;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Jd=class{route;type=at.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},eu=class{route;type=at.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},tu=class{snapshot;type=at.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},nu=class{snapshot;type=at.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},iu=class{snapshot;type=at.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ru=class{snapshot;type=at.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Oo=class{},ka=class{},Fo=class{url;navigationBehaviorOptions;constructor(n,t){this.url=n,this.navigationBehaviorOptions=t}};function oO(e){return!(e instanceof Oo)&&!(e instanceof Fo)&&!(e instanceof ka)}var ou=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Vo(this.rootInjector)}},Vo=(()=>{class e{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new ou(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||e)(I(Ee))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),su=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let t=this.pathFromRoot(n);return t.length>1?t[t.length-2]:null}children(n){let t=xg(n,this._root);return t?t.children.map(i=>i.value):[]}firstChild(n){let t=xg(n,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(n){let t=Sg(n,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Sg(n,this._root).map(t=>t.value)}};function xg(e,n){if(e===n.value)return n;for(let t of n.children){let i=xg(e,t);if(i)return i}return null}function Sg(e,n){if(e===n.value)return[n];for(let t of n.children){let i=Sg(e,t);if(i.length)return i.unshift(n),i}return[]}var Ft=class{value;children;constructor(n,t){this.value=n,this.children=t}toString(){return`TreeNode(${this.value})`}};function Ao(e){let n={};return e&&e.children.forEach(t=>n[t.value.outlet]=t),n}var Na=class extends su{snapshot;constructor(n,t){super(n),this.snapshot=t,Pg(this,n)}toString(){return this.snapshot.toString()}};function eC(e,n){let t=sO(e,n),i=new Pe([new Ui("",{})]),r=new Pe({}),o=new Pe({}),s=new Pe({}),a=new Pe(""),c=new Un(i,r,s,a,o,J,e,t.root);return c.snapshot=t.root,new Na(new Ft(c,[]),t)}function sO(e,n){let t={},i={},r={},s=new Po([],t,r,"",i,J,e,null,{},n);return new Oa("",new Ft(s,[]))}var Un=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,t,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(T(l=>l[La]))??R(void 0),this.url=n,this.params=t,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(T(n=>Ar(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(T(n=>Ar(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Fg(e,n,t="emptyOnly"){let i,{routeConfig:r}=e;return n!==null&&(t==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:_(_({},n.params),e.params),data:_(_({},n.data),e.data),resolve:_(_(_(_({},e.data),n.data),r?.data),e._resolvedData)}:i={params:_({},e.params),data:_({},e.data),resolve:_(_({},e.data),e._resolvedData??{})},r&&nC(r)&&(i.resolve[La]=r.title),i}var Po=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[La]}constructor(n,t,i,r,o,s,a,c,l,d){this.url=n,this.params=t,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Ar(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Ar(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${t}')`}},Oa=class extends su{url;constructor(n,t){super(t),this.url=n,Pg(this,t)}toString(){return tC(this._root)}};function Pg(e,n){n.value._routerState=e,n.children.forEach(t=>Pg(e,t))}function tC(e){let n=e.children.length>0?` { ${e.children.map(tC).join(", ")} } `:"";return`${e.value}${n}`}function _g(e){if(e.snapshot){let n=e.snapshot,t=e._futureSnapshot;e.snapshot=t,Bn(n.queryParams,t.queryParams)||e.queryParamsSubject.next(t.queryParams),n.fragment!==t.fragment&&e.fragmentSubject.next(t.fragment),Bn(n.params,t.params)||e.paramsSubject.next(t.params),ON(n.url,t.url)||e.urlSubject.next(t.url),Bn(n.data,t.data)||e.dataSubject.next(t.data)}else e.snapshot=e._futureSnapshot,e.dataSubject.next(e._futureSnapshot.data)}function Mg(e,n){let t=Bn(e.params,n.params)&&VN(e.url,n.url),i=!e.parent!=!n.parent;return t&&!i&&(!e.parent||Mg(e.parent,n.parent))}function nC(e){return typeof e.title=="string"||e.title===null}var iC=new y(""),Va=(()=>{class e{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=J;activateEvents=new oe;deactivateEvents=new oe;attachEvents=new oe;detachEvents=new oe;routerOutletData=kw();parentContexts=f(Vo);location=f(yn);changeDetector=f(ht);inputBinder=f(du,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new x(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new x(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new x(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new x(4013,!1);this._activatedRoute=t;let r=this.location,s=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Tg(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[xt]})}return e})(),Tg=class{route;childContexts;parent;outletData;constructor(n,t,i,r){this.route=n,this.childContexts=t,this.parent=i,this.outletData=r}get(n,t){return n===Un?this.route:n===Vo?this.childContexts:n===iC?this.outletData:this.parent.get(n,t)}},du=new y("");var Lg=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&Ce(0,"router-outlet")},dependencies:[Va],encapsulation:2})}return e})();function jg(e){let n=e.children&&e.children.map(jg),t=n?A(_({},e),{children:n}):_({},e);return!t.component&&!t.loadComponent&&(n||t.loadChildren)&&t.outlet&&t.outlet!==J&&(t.component=Lg),t}function aO(e,n,t){let i=Fa(e,n._root,t?t._root:void 0);return new Na(i,n)}function Fa(e,n,t){if(t&&e.shouldReuseRoute(n.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=n.value;let r=cO(e,n,t);return new Ft(i,r)}else{if(e.shouldAttach(n.value)){let o=e.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>Fa(e,a)),s}}let i=lO(n.value),r=n.children.map(o=>Fa(e,o));return new Ft(i,r)}}function cO(e,n,t){return n.children.map(i=>{for(let r of t.children)if(e.shouldReuseRoute(i.value,r.value.snapshot))return Fa(e,i,r);return Fa(e,i)})}function lO(e){return new Un(new Pe(e.url),new Pe(e.params),new Pe(e.queryParams),new Pe(e.fragment),new Pe(e.data),e.outlet,e.component,e)}var Lo=class{redirectTo;navigationBehaviorOptions;constructor(n,t){this.redirectTo=n,this.navigationBehaviorOptions=t}},rC="ngNavigationCancelingError";function au(e,n){let{redirectTo:t,navigationBehaviorOptions:i}=No(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=oC(!1,_t.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function oC(e,n){let t=new Error(`NavigationCancelingError: ${e||""}`);return t[rC]=!0,t.cancellationCode=n,t}function dO(e){return sC(e)&&No(e.url)}function sC(e){return!!e&&e[rC]}var Ag=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,t,i,r,o){this.routeReuseStrategy=n,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,n),_g(this.futureState.root),this.activateChildRoutes(t,i,n)}deactivateChildRoutes(n,t,i){let r=Ao(t);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,t,i){let r=n.value,o=t?t.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,t,s.children)}else this.deactivateChildRoutes(n,t,i);else o&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(n,t){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,t):this.deactivateRouteAndOutlet(n,t)}detachAndStoreRouteSubtree(n,t){let i=t.getContext(n.value.outlet),r=i&&n.value.component?i.children:t,o=Ao(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,t){let i=t.getContext(n.value.outlet),r=i&&n.value.component?i.children:t,o=Ao(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,t,i){let r=Ao(t);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new ru(o.value.snapshot))}),n.children.length&&this.forwardEvent(new nu(n.value.snapshot))}activateRoutes(n,t,i){let r=n.value,o=t?t.value:null;if(_g(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,t,s.children)}else this.activateChildRoutes(n,t,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),_g(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},cu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ko=class{component;route;constructor(n,t){this.component=n,this.route=t}};function uO(e,n,t){let i=e._root,r=n?n._root:null;return Ia(i,r,t,[i.value])}function fO(e){let n=e.routeConfig?e.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:e,guards:n}}function Bo(e,n){let t=Symbol(),i=n.get(e,t);return i===t?typeof e=="function"&&!vh(e)?e:n.get(e):i}function Ia(e,n,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Ao(n);return e.children.forEach(s=>{hO(s,o[s.value.outlet],t,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Ma(a,t.getContext(s),r)),r}function hO(e,n,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=e.value,s=n?n.value:null,a=t?t.getContext(e.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=pO(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new cu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Ia(e,n,a?a.children:null,i,r):Ia(e,n,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ko(a.outlet.component,s))}else s&&Ma(n,a,r),r.canActivateChecks.push(new cu(i)),o.component?Ia(e,null,a?a.children:null,i,r):Ia(e,null,t,i,r);return r}function pO(e,n,t){if(typeof t=="function")return et(n._environmentInjector,()=>t(e,n));switch(t){case"pathParamsChange":return!Tr(e.url,n.url);case"pathParamsOrQueryParamsChange":return!Tr(e.url,n.url)||!Bn(e.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Mg(e,n)||!Bn(e.queryParams,n.queryParams);default:return!Mg(e,n)}}function Ma(e,n,t){let i=Ao(e),r=e.value;Object.entries(i).forEach(([o,s])=>{r.component?n?Ma(s,n.children.getContext(o),t):Ma(s,null,t):Ma(s,n,t)}),r.component?n&&n.outlet&&n.outlet.isActivated?t.canDeactivateChecks.push(new ko(n.outlet.component,r)):t.canDeactivateChecks.push(new ko(null,r)):t.canDeactivateChecks.push(new ko(null,r))}function Ba(e){return typeof e=="function"}function mO(e){return typeof e=="boolean"}function gO(e){return e&&Ba(e.canLoad)}function vO(e){return e&&Ba(e.canActivate)}function yO(e){return e&&Ba(e.canActivateChild)}function _O(e){return e&&Ba(e.canDeactivate)}function bO(e){return e&&Ba(e.canMatch)}function aC(e){return e instanceof tr||e?.name==="EmptyError"}var Hd=Symbol("INITIAL_VALUE");function jo(){return He(e=>ms(e.map(n=>n.pipe(ct(1),cn(Hd)))).pipe(T(n=>{for(let t of n)if(t!==!0){if(t===Hd)return Hd;if(t===!1||wO(t))return t}return!0}),_e(n=>n!==Hd),ct(1)))}function wO(e){return No(e)||e instanceof Lo}function cC(e){return e.aborted?R(void 0).pipe(ct(1)):new H(n=>{let t=()=>{n.next(),n.complete()};return e.addEventListener("abort",t),()=>e.removeEventListener("abort",t)})}function lC(e){return Ne(cC(e))}function EO(e){return Xe(n=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?R(A(_({},n),{guardsResult:!0})):CO(o,t,i).pipe(Xe(s=>s&&mO(s)?DO(t,r,e):R(s)),T(s=>A(_({},n),{guardsResult:s})))})}function CO(e,n,t){return ke(e).pipe(Xe(i=>TO(i.component,i.route,t,n)),qn(i=>i!==!0,!0))}function DO(e,n,t){return ke(n).pipe(wi(i=>bi(xO(i.route.parent,t),IO(i.route,t),MO(e,i.path),SO(e,i.route))),qn(i=>i!==!0,!0))}function IO(e,n){return e!==null&&n&&n(new iu(e)),R(!0)}function xO(e,n){return e!==null&&n&&n(new tu(e)),R(!0)}function SO(e,n){let t=n.routeConfig?n.routeConfig.canActivate:null;if(!t||t.length===0)return R(!0);let i=t.map(r=>gs(()=>{let o=n._environmentInjector,s=Bo(r,o),a=vO(s)?s.canActivate(n,e):et(o,()=>s(n,e));return Nr(a).pipe(qn())}));return R(i).pipe(jo())}function MO(e,n){let t=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>fO(o)).filter(o=>o!==null).map(o=>gs(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Bo(a,c),d=yO(l)?l.canActivateChild(t,e):et(c,()=>l(t,e));return Nr(d).pipe(qn())});return R(s).pipe(jo())}));return R(r).pipe(jo())}function TO(e,n,t,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return R(!0);let o=r.map(s=>{let a=n._environmentInjector,c=Bo(s,a),l=_O(c)?c.canDeactivate(e,n,t,i):et(a,()=>c(e,n,t,i));return Nr(l).pipe(qn())});return R(o).pipe(jo())}function AO(e,n,t,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return R(!0);let s=o.map(a=>{let c=Bo(a,e),l=gO(c)?c.canLoad(n,t):et(e,()=>c(n,t)),d=Nr(l);return r?d.pipe(lC(r)):d});return R(s).pipe(jo(),dC(i))}function dC(e){return zf(Je(n=>{if(typeof n!="boolean")throw au(e,n)}),T(n=>n===!0))}function RO(e,n,t,i,r,o){let s=n.canMatch;if(!s||s.length===0)return R(!0);let a=s.map(c=>{let l=Bo(c,e),d=bO(l)?l.canMatch(n,t,r):et(e,()=>l(n,t,r));return Nr(d).pipe(lC(o))});return R(a).pipe(jo(),dC(i))}var di=class e extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,e.prototype)}},Pa=class e extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,e.prototype)}};function kO(e){throw new x(4e3,!1)}function NO(e){throw oC(!1,_t.GuardRejected)}var Rg=class{urlSerializer;urlTree;constructor(n,t){this.urlSerializer=n,this.urlTree=t}lineralizeSegments(n,t){return Se(this,null,function*(){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[J])throw kO(`${n.redirectTo}`);r=r.children[J]}})}applyRedirectCommands(n,t,i,r,o){return Se(this,null,function*(){let s=yield OO(t,r,o);if(s instanceof rn)throw new Pa(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Pa(a);return a})}applyRedirectCreateUrlTree(n,t,i,r){let o=this.createSegmentGroup(n,t.root,i,r);return new rn(o,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(n,t){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=t[a]}else i[r]=o}),i}createSegmentGroup(n,t,i,r){let o=this.createSegments(n,t.segments,i,r),s={};return Object.entries(t.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new me(o,s)}createSegments(n,t,i,r){return t.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,t,i){let r=i[t.path.substring(1)];if(!r)throw new x(4001,!1);return r}findOrReturn(n,t){let i=0;for(let r of t){if(r.path===n.path)return t.splice(i),r;i++}return n}};function OO(e,n,t){if(typeof e=="string")return Promise.resolve(e);let i=e;return Wd(Nr(et(t,()=>i(n))))}function FO(e,n){return e.providers&&!e._injector&&(e._injector=ta(e.providers,n,`Route: ${e.path}`)),e._injector??n}function Cn(e){return e.outlet||J}function PO(e,n){let t=e.filter(i=>Cn(i)===n);return t.push(...e.filter(i=>Cn(i)!==n)),t}var kg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function uC(e){return{routeConfig:e.routeConfig,url:e.url,params:e.params,queryParams:e.queryParams,fragment:e.fragment,data:e.data,outlet:e.outlet,title:e.title,paramMap:e.paramMap,queryParamMap:e.queryParamMap}}function LO(e,n,t,i,r,o,s){let a=fC(e,n,t);if(!a.matched)return R(a);let c=uC(o(a));return i=FO(n,i),RO(i,n,t,r,c,s).pipe(T(l=>l===!0?a:_({},kg)))}function fC(e,n,t){if(n.path==="")return n.pathMatch==="full"&&(e.hasChildren()||t.length>0)?_({},kg):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(n.matcher||PE)(t,e,n);if(!r)return _({},kg);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?_(_({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function OE(e,n,t,i,r){return t.length>0&&BO(e,t,i,r)?{segmentGroup:new me(n,VO(i,new me(t,e.children))),slicedSegments:[]}:t.length===0&&UO(e,t,i)?{segmentGroup:new me(e.segments,jO(e,t,i,e.children)),slicedSegments:t}:{segmentGroup:new me(e.segments,e.children),slicedSegments:t}}function jO(e,n,t,i){let r={};for(let o of t)if(uu(e,n,o)&&!i[Cn(o)]){let s=new me([],{});r[Cn(o)]=s}return _(_({},i),r)}function VO(e,n){let t={};t[J]=n;for(let i of e)if(i.path===""&&Cn(i)!==J){let r=new me([],{});t[Cn(i)]=r}return t}function BO(e,n,t,i){return t.some(r=>!uu(e,n,r)||!(Cn(r)!==J)?!1:!(i!==void 0&&Cn(r)===i))}function UO(e,n,t){return t.some(i=>uu(e,n,i))}function uu(e,n,t){return(e.hasChildren()||n.length>0)&&t.pathMatch==="full"?!1:t.path===""}function HO(e,n,t){return n.length===0&&!e.children[t]}var Ng=class{};function $O(e,n,t,i,r,o,s="emptyOnly",a){return Se(this,null,function*(){return new Og(e,n,t,i,r,s,o,a).recognize()})}var zO=31,Og=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,t,i,r,o,s,a,c){this.injector=n,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Rg(this.urlSerializer,this.urlTree)}noMatchError(n){return new x(4002,`'${n.segmentGroup}'`)}recognize(){return Se(this,null,function*(){let n=OE(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=yield this.match(n),r=new Ft(i,t),o=new Oa("",r),s=YE(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}})}match(n){return Se(this,null,function*(){let t=new Po([],Object.freeze({}),Object.freeze(_({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),J,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,n,J,t),rootSnapshot:t}}catch(i){if(i instanceof Pa)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof di?this.noMatchError(i):i}})}processSegmentGroup(n,t,i,r,o){return Se(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,t,i,o);let s=yield this.processSegment(n,t,i,i.segments,r,!0,o);return s instanceof Ft?[s]:[]})}processChildren(n,t,i,r){return Se(this,null,function*(){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=PO(t,c),u=yield this.processSegmentGroup(n,d,l,c,r);s.push(...u)}let a=hC(s);return GO(a),a})}processSegment(n,t,i,r,o,s,a){return Se(this,null,function*(){for(let c of t)try{return yield this.processSegmentAgainstRoute(c._injector??n,t,c,i,r,o,s,a)}catch(l){if(l instanceof di||aC(l))continue;throw l}if(HO(i,r,o))return new Ng;throw new di(i)})}processSegmentAgainstRoute(n,t,i,r,o,s,a,c){return Se(this,null,function*(){if(Cn(i)!==s&&(s===J||!uu(r,o,i)))throw new di(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,t,i,o,s,c);throw new di(r)})}expandSegmentAgainstRouteUsingRedirect(n,t,i,r,o,s,a){return Se(this,null,function*(){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:u,remainingSegments:h}=fC(t,r,o);if(!c)throw new di(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>zO&&(this.allowRedirects=!1));let p=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let m=yield this.applyRedirects.applyRedirectCommands(d,r.redirectTo,u,uC(p),n),b=yield this.applyRedirects.lineralizeSegments(r,m);return this.processSegment(n,i,t,b.concat(h),s,!1,a)})}createSnapshot(n,t,i,r,o){let s=new Po(i,r,Object.freeze(_({},this.urlTree.queryParams)),this.urlTree.fragment,qO(t),Cn(t),t.component??t._loadedComponent??null,t,YO(t),n),a=Fg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}matchSegmentAgainstRoute(n,t,i,r,o,s){return Se(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=ue=>this.createSnapshot(n,i,ue.consumedSegments,ue.parameters,s),c=yield Wd(LO(t,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new di(t);n=i._injector??n;let{routes:l}=yield this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:u,consumedSegments:h,remainingSegments:p}=c,m=this.createSnapshot(n,i,h,u,s),{segmentGroup:b,slicedSegments:D}=OE(t,h,p,l,o);if(D.length===0&&b.hasChildren()){let ue=yield this.processChildren(d,l,b,m);return new Ft(m,ue)}if(l.length===0&&D.length===0)return new Ft(m,[]);let S=Cn(i)===o,L=yield this.processSegment(d,l,b,D,S?J:o,!0,m);return new Ft(m,L instanceof Ft?[L]:[])})}getChildConfig(n,t,i){return Se(this,null,function*(){if(t.children)return{routes:t.children,injector:n};if(t.loadChildren){if(t._loadedRoutes!==void 0){let o=t._loadedNgModuleFactory;return o&&!t._loadedInjector&&(t._loadedInjector=o.create(n).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield Wd(AO(n,t,i,this.urlSerializer,this.abortSignal))){let o=yield this.configLoader.loadChildren(n,t);return t._loadedRoutes=o.routes,t._loadedInjector=o.injector,t._loadedNgModuleFactory=o.factory,o}throw NO(t)}return{routes:[],injector:n}})}};function GO(e){e.sort((n,t)=>n.value.outlet===J?-1:t.value.outlet===J?1:n.value.outlet.localeCompare(t.value.outlet))}function WO(e){let n=e.value.routeConfig;return n&&n.path===""}function hC(e){let n=[],t=new Set;for(let i of e){if(!WO(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):n.push(i)}for(let i of t){let r=hC(i.children);n.push(new Ft(i.value,r))}return n.filter(i=>!t.has(i))}function qO(e){return e.data||{}}function YO(e){return e.resolve||{}}function ZO(e,n,t,i,r,o,s){return Xe(a=>Se(null,null,function*(){let{state:c,tree:l}=yield $O(e,n,t,i,a.extractedUrl,r,o,s);return A(_({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function KO(e){return Xe(n=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=n;if(!i.length)return R(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of pC(a))o.add(c);let s=0;return ke(o).pipe(wi(a=>r.has(a)?QO(a,t,e):(a.data=Fg(a,a.parent,e).resolve,R(void 0))),Je(()=>s++),Jc(1),Xe(a=>s===o.size?R(n):xe))})}function pC(e){let n=e.children.map(t=>pC(t)).flat();return[e,...n]}function QO(e,n,t){let i=e.routeConfig,r=e._resolve;return i?.title!==void 0&&!nC(i)&&(r[La]=i.title),gs(()=>(e.data=Fg(e,e.parent,t).resolve,XO(r,e,n).pipe(T(o=>(e._resolvedData=o,e.data=_(_({},e.data),o),null)))))}function XO(e,n,t){let i=wg(e);if(i.length===0)return R({});let r={};return ke(i).pipe(Xe(o=>JO(e[o],n,t).pipe(qn(),Je(s=>{if(s instanceof Lo)throw au(new Hi,s);r[o]=s}))),Jc(1),T(()=>r),Et(o=>aC(o)?xe:er(o)))}function JO(e,n,t){let i=n._environmentInjector,r=Bo(e,i),o=r.resolve?r.resolve(n,t):et(i,()=>r(n,t));return Nr(o)}function FE(e){return He(n=>{let t=e(n);return t?ke(t).pipe(T(()=>n)):R(n)})}var Vg=(()=>{class e{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===J);return i}getResolvedTitleForRoute(t){return t.data[La]}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>f(mC),providedIn:"root"})}return e})(),mC=(()=>{class e extends Vg{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||e)(I(SE))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Ua=new y("",{factory:()=>({})}),Ha=new y(""),gC=(()=>{class e{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=f(jm);loadComponent(t,i){return Se(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=Se(this,null,function*(){try{let o=yield jE(et(t,()=>i.loadComponent())),s=yield _C(yC(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=Se(this,null,function*(){try{let o=yield vC(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function vC(e,n,t,i){return Se(this,null,function*(){let r=yield jE(et(t,()=>e.loadChildren())),o=yield _C(yC(r)),s;o instanceof fd||Array.isArray(o)?s=o:s=yield n.compileModuleAsync(o),i&&i(e);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(t).injector,d=s,c=a.get(Ha,[],{optional:!0,self:!0}).flat()),{routes:c.map(jg),injector:a,factory:d}})}function eF(e){return e&&typeof e=="object"&&"default"in e}function yC(e){return eF(e)?e.default:e}function _C(e){return Se(this,null,function*(){return e})}var fu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>f(tF),providedIn:"root"})}return e})(),tF=(()=>{class e{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),bC=new y("");var nF=()=>{},wC=new y(""),EC=(()=>{class e{currentNavigation=Te(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Te(null);events=new M;transitionAbortWithErrorSubject=new M;configLoader=f(gC);environmentInjector=f(Ee);destroyRef=f(dt);urlSerializer=f(ja);rootContexts=f(Vo);location=f(Vi);inputBindingEnabled=f(du,{optional:!0})!==null;titleStrategy=f(Vg);options=f(Ua,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=f(fu);createViewTransition=f(bC,{optional:!0});navigationErrorHandler=f(wC,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>R(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Jd(r)),i=r=>this.events.next(new eu(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;je(()=>{this.transitions?.next(A(_({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new Pe(null),this.transitions.pipe(_e(i=>i!==null),He(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return R(i).pipe(He(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",_t.SupersededByNewNavigation),xe;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?A(_({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),d=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&d!=="reload")return this.events.next(new fi(a.id,this.urlSerializer.serialize(a.rawUrl),"",Aa.IgnoredSameUrlNavigation)),a.resolve(!1),xe;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return R(a).pipe(He(u=>(this.events.next(new Rr(u.id,this.urlSerializer.serialize(u.extractedUrl),u.source,u.restoredState)),u.id!==this.navigationId?xe:Promise.resolve(u))),ZO(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),Je(u=>{i.targetSnapshot=u.targetSnapshot,i.urlAfterRedirects=u.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=u.urlAfterRedirects,h)),this.events.next(new ka)}),He(u=>ke(i.routesRecognizeHandler.deferredHandle??R(void 0)).pipe(T(()=>u))),Je(()=>{let u=new Ra(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(u)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:u,extractedUrl:h,source:p,restoredState:m,extras:b}=a,D=new Rr(u,this.urlSerializer.serialize(h),p,m);this.events.next(D);let S=eC(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=A(_({},a),{targetSnapshot:S,urlAfterRedirects:h,extras:A(_({},b),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(L=>(L.finalUrl=h,L)),R(i)}else return this.events.next(new fi(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Aa.IgnoredByUrlHandlingStrategy)),a.resolve(!1),xe}),T(a=>{let c=new Zd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=A(_({},a),{guards:uO(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),EO(a=>this.events.next(a)),He(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw au(this.urlSerializer,a.guardsResult);let c=new Kd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return xe;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",_t.GuardRejected),xe;if(a.guards.canActivateChecks.length===0)return R(a);let l=new Qd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return xe;let d=!1;return R(a).pipe(KO(this.paramsInheritanceStrategy),Je({next:()=>{d=!0;let u=new Xd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(u)},complete:()=>{d||this.cancelNavigationTransition(a,"",_t.NoDataFromResolver)}}))}),FE(a=>{let c=d=>{let u=[];if(d.routeConfig?._loadedComponent)d.component=d.routeConfig?._loadedComponent;else if(d.routeConfig?.loadComponent){let h=d._environmentInjector;u.push(this.configLoader.loadComponent(h,d.routeConfig).then(p=>{d.component=p}))}for(let h of d.children)u.push(...c(h));return u},l=c(a.targetSnapshot.root);return l.length===0?R(a):ke(Promise.all(l).then(()=>a))}),FE(()=>this.afterPreactivation()),He(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?ke(l).pipe(T(()=>i)):R(i)}),ct(1),He(a=>{let c=aO(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=A(_({},a),{targetRouterState:c}),this.currentNavigation.update(d=>(d.targetRouterState=c,d)),this.events.next(new Oo);let l=i.beforeActivateHandler.deferredHandle;return l?ke(l.then(()=>a)):R(a)}),Je(a=>{new Ag(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(c=>(c.abort=nF,c)),this.lastSuccessfulNavigation.set(je(this.currentNavigation)),this.events.next(new ui(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Ne(cC(o.signal).pipe(_e(()=>!r&&!i.targetRouterState),Je(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",_t.Aborted)}))),Je({complete:()=>{r=!0}}),Ne(this.transitionAbortWithErrorSubject.pipe(Je(a=>{throw a}))),ir(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",_t.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Et(a=>{if(r=!0,this.destroyed)return i.resolve(!1),xe;if(sC(a))this.events.next(new nn(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),dO(a)?this.events.next(new Fo(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new kr(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=et(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof Lo){let{message:d,cancellationCode:u}=au(this.urlSerializer,l);this.events.next(new nn(i.id,this.urlSerializer.serialize(i.extractedUrl),d,u)),this.events.next(new Fo(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return xe}))}))}cancelNavigationTransition(t,i,r){let o=new nn(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(o),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=je(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function iF(e){return e!==Sa}var CC=new y("");var DC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>f(rF),providedIn:"root"})}return e})(),lu=class{shouldDetach(n){return!1}store(n,t){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,t){return n.routeConfig===t.routeConfig}shouldDestroyInjector(n){return!0}},rF=(()=>{class e extends lu{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Bg=(()=>{class e{urlSerializer=f(ja);options=f(Ua,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=f(Vi);urlHandlingStrategy=f(fu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new rn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let o=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,s=r??o;return s instanceof rn?this.urlSerializer.serialize(s):s}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=eC(null,f(Ee));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:()=>f(oF),providedIn:"root"})}return e})(),oF=(()=>{class e extends Bg{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof Rr?this.updateStateMemento():t instanceof fi?this.commitTransition(i):t instanceof Ra?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Oo?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof nn&&!JE(t)?this.restoreHistory(i):t instanceof kr?this.restoreHistory(i,!0):t instanceof ui&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(t)||s){let c=this.browserPageId,l=_(_({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(t,"",l)}else{let c=_(_({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(t,"",c)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===t.finalUrl&&o===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i,r){return this.canceledNavigationResolution==="computed"?_({navigationId:t,\u0275routerPageId:i},this.routerUrlState(r)):_({navigationId:t},this.routerUrlState(r))}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Ug(e,n){e.events.pipe(_e(t=>t instanceof ui||t instanceof nn||t instanceof kr||t instanceof fi),T(t=>t instanceof ui||t instanceof fi?0:(t instanceof nn?t.code===_t.Redirect||t.code===_t.SupersededByNewNavigation:!1)?2:1),_e(t=>t!==2),ct(1)).subscribe(()=>{n()})}var $i=(()=>{class e{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=f(hd);stateManager=f(Bg);options=f(Ua,{optional:!0})||{};pendingTasks=f(ti);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=f(EC);urlSerializer=f(ja);location=f(Vi);urlHandlingStrategy=f(fu);injector=f(Ee);_events=new M;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=f(DC);injectorCleanup=f(CC,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=f(Ha,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!f(du,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new Ie;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=je(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof nn&&i.code!==_t.Redirect&&i.code!==_t.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ui)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Fo){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=_({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||iF(r.source)},s);this.scheduleNavigation(a,Sa,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}oO(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Sa,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,o)=>{this.navigateToSyncWithBrowser(t,r,i,o)})}navigateToSyncWithBrowser(t,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??t;if(r?.\u0275routerUrl&&(o=A(_({},o),{browserUrl:t})),r){let l=_({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Wt)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return je(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(jg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=_(_({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let u;try{let h=r?r.snapshot:this.routerState.snapshot.root;u=ZE(h)}catch(h){(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),u=this.currentUrlTree.root}return KE(u,t,d,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=No(t)?t:this.parseUrl(t),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Sa,null,i)}navigate(t,i={skipLocationChange:!1}){return sF(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch(i){return this.console.warn(Mn(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=_({},BE):i===!1?r=_({},Eg):r=_(_({},Eg),i),No(t))return TE(this.currentUrlTree,t,r);let o=this.parseUrl(t);return TE(this.currentUrlTree,o,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(t,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((u,h)=>{a=u,c=h});let d=this.pendingTasks.add();return Ug(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function sF(e){for(let n=0;n<e.length;n++)if(e[n]==null)throw new x(4008,!1)}var lF=new y("");function Hg(e,...n){return Tt([{provide:Ha,multi:!0,useValue:e},[],{provide:Un,useFactory:dF},{provide:md,multi:!0,useFactory:uF},n.map(t=>t.\u0275providers)])}function dF(){return f($i).routerState.root}function uF(){let e=f(re);return n=>{let t=e.get(Kt);if(n!==t.components[0])return;let i=e.get($i),r=e.get(fF);e.get(hF)===1&&i.initialNavigation(),e.get(pF,null,{optional:!0})?.setUpPreloading(),e.get(lF,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var fF=new y("",{factory:()=>new M}),hF=new y("",{factory:()=>1});var pF=new y("");function hu(e,n){let i=!n?.manualCleanup?n?.injector?.get(dt)??f(dt):null,r=gF(n?.equal),o;n?.requireSync?o=Te({kind:0},{equal:r}):o=Te({kind:1,value:n?.initialValue},{equal:r});let s,a=e.subscribe({next:c=>o.set({kind:1,value:c}),error:c=>{o.set({kind:2,error:c}),s?.()},complete:()=>{s?.()}});if(n?.requireSync&&o().kind===0)throw new x(601,!1);return s=i?.onDestroy(a.unsubscribe.bind(a)),Ge(()=>{let c=o();switch(c.kind){case 1:return c.value;case 2:throw c.error;case 0:throw new x(601,!1)}},{equal:n?.equal})}function gF(e=Object.is){return(n,t)=>n.kind===1&&t.kind===1&&e(n.value,t.value)}var Gg={};function Hn(e,n){if(Gg[e]=(Gg[e]||0)+1,typeof n=="function")return $g(e,(...i)=>A(_({},n(...i)),{type:e}));switch(n?n._as:"empty"){case"empty":return $g(e,()=>({type:e}));case"props":return $g(e,i=>A(_({},i),{type:e}));default:throw new Error("Unexpected config.")}}function $o(){return{_as:"props",_p:void 0}}function $g(e,n){return Object.defineProperty(n,"type",{value:e,writable:!1})}function vF(e,n){if(e==null)throw new Error(`${n} must be defined.`)}var Ga="@ngrx/store/init",$n=(()=>{class e extends Pe{constructor(){super({type:Ga})}next(t){if(typeof t=="function")throw new TypeError(`
        Dispatch expected an object, instead it received a function.
        If you're using the createAction function, make sure to invoke the function
        before dispatching the action. For example, someAction should be someAction().`);if(typeof t>"u")throw new TypeError("Actions must be objects");if(typeof t.type>"u")throw new TypeError("Actions must have a type property");super.next(t)}complete(){}ngOnDestroy(){super.complete()}static{this.\u0275fac=function(i){return new(i||e)}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})(),yF=[$n],jC=new y("@ngrx/store Internal Root Guard"),xC=new y("@ngrx/store Internal Initial State"),Wa=new y("@ngrx/store Initial State"),VC=new y("@ngrx/store Reducer Factory"),SC=new y("@ngrx/store Internal Reducer Factory Provider"),BC=new y("@ngrx/store Initial Reducers"),zg=new y("@ngrx/store Internal Initial Reducers");var MC=new y("@ngrx/store Internal Store Reducers");var _F=new y("@ngrx/store Internal Store Features");var bF=new y("@ngrx/store Feature Reducers"),TC=new y("@ngrx/store User Provided Meta Reducers"),pu=new y("@ngrx/store Meta Reducers"),AC=new y("@ngrx/store Internal Resolved Meta Reducers"),RC=new y("@ngrx/store User Runtime Checks Config"),kC=new y("@ngrx/store Internal User Runtime Checks Config"),$a=new y("@ngrx/store Internal Runtime Checks"),Zg=new y("@ngrx/store Check if Action types are unique"),za=new y("@ngrx/store Root Store Provider"),mu=new y("@ngrx/store Feature State Provider");function wF(e,n={}){let t=Object.keys(e),i={};for(let o=0;o<t.length;o++){let s=t[o];typeof e[s]=="function"&&(i[s]=e[s])}let r=Object.keys(i);return function(s,a){s=s===void 0?n:s;let c=!1,l={};for(let d=0;d<r.length;d++){let u=r[d],h=i[u],p=s[u],m=h(p,a);l[u]=m,c=c||m!==p}return c?l:s}}function EF(e,n){return Object.keys(e).filter(t=>t!==n).reduce((t,i)=>Object.assign(t,{[i]:e[i]}),{})}function UC(...e){return function(n){if(e.length===0)return n;let t=e[e.length-1];return e.slice(0,-1).reduceRight((r,o)=>o(r),t(n))}}function HC(e,n){return Array.isArray(n)&&n.length>0&&(e=UC.apply(null,[...n,e])),(t,i)=>{let r=e(t);return(o,s)=>(o=o===void 0?i:o,r(o,s))}}function CF(e){let n=Array.isArray(e)&&e.length>0?UC(...e):t=>t;return(t,i)=>(t=n(t),(r,o)=>(r=r===void 0?i:r,t(r,o)))}var Or=class extends H{},Uo=class extends $n{},vu="@ngrx/store/update-reducers",gu=(()=>{class e extends Pe{get currentReducers(){return this.reducers}constructor(t,i,r,o){super(o(r,i)),this.dispatcher=t,this.initialState=i,this.reducers=r,this.reducerFactory=o}addFeature(t){this.addFeatures([t])}addFeatures(t){let i=t.reduce((r,{reducers:o,reducerFactory:s,metaReducers:a,initialState:c,key:l})=>{let d=typeof o=="function"?CF(a)(o,c):HC(s,a)(o,c);return r[l]=d,r},{});this.addReducers(i)}removeFeature(t){this.removeFeatures([t])}removeFeatures(t){this.removeReducers(t.map(i=>i.key))}addReducer(t,i){this.addReducers({[t]:i})}addReducers(t){this.reducers=_(_({},this.reducers),t),this.updateReducers(Object.keys(t))}removeReducer(t){this.removeReducers([t])}removeReducers(t){t.forEach(i=>{this.reducers=EF(this.reducers,i)}),this.updateReducers(t)}updateReducers(t){this.next(this.reducerFactory(this.reducers,this.initialState)),this.dispatcher.next({type:vu,features:t})}ngOnDestroy(){this.complete()}static{this.\u0275fac=function(i){return new(i||e)(I(Uo),I(Wa),I(BC),I(VC))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})(),DF=[gu,{provide:Or,useExisting:gu},{provide:Uo,useExisting:$n}],Fr=(()=>{class e extends M{ngOnDestroy(){this.complete()}static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})()}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})(),IF=[Fr],Ho=class extends H{},NC=(()=>{class e extends Pe{static{this.INIT=Ga}constructor(t,i,r,o){super(o);let a=t.pipe(Wn(ps)).pipe(bs(i)),c={state:o},l=a.pipe(_s(xF,c));this.stateSubscription=l.subscribe(({state:d,action:u})=>{this.next(d),r.next(u)}),this.state=hu(this,{manualCleanup:!0,requireSync:!0})}ngOnDestroy(){this.stateSubscription.unsubscribe(),this.complete()}static{this.\u0275fac=function(i){return new(i||e)(I($n),I(Or),I(Fr),I(Wa))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})();function xF(e={state:void 0},[n,t]){let{state:i}=e;return{state:t(i,n),action:n}}var SF=[NC,{provide:Ho,useExisting:NC}],Lt=(()=>{class e extends H{constructor(t,i,r,o){super(),this.actionsObserver=i,this.reducerManager=r,this.injector=o,this.source=t,this.state=t.state}select(t,...i){return TF.call(null,t,...i)(this)}selectSignal(t,i){return Ge(()=>t(this.state()),i)}lift(t){let i=new e(this,this.actionsObserver,this.reducerManager);return i.operator=t,i}dispatch(t,i){if(typeof t=="function")return this.processDispatchFn(t,i);this.actionsObserver.next(t)}next(t){this.actionsObserver.next(t)}error(t){this.actionsObserver.error(t)}complete(){this.actionsObserver.complete()}addReducer(t,i){this.reducerManager.addReducer(t,i)}removeReducer(t){this.reducerManager.removeReducer(t)}processDispatchFn(t,i){vF(this.injector,"Store Injector");let r=i?.injector??AF()??this.injector;return hn(()=>{let o=t();je(()=>this.dispatch(o))},{injector:r})}static{this.\u0275fac=function(i){return new(i||e)(I(Ho),I($n),I(gu),I(re))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})(),MF=[Lt];function TF(e,n,...t){return function(r){let o;if(typeof e=="string"){let s=[n,...t].filter(Boolean);o=r.pipe(Jf(e,...s))}else if(typeof e=="function")o=r.pipe(T(s=>e(s,n)));else throw new TypeError(`Unexpected type '${typeof e}' in select operator, expected 'string' or 'function'`);return o.pipe(ys())}}function AF(){try{return f(re)}catch(e){return}}var Kg="https://ngrx.io/guide/store/configuration/runtime-checks";function OC(e){return e===void 0}function FC(e){return e===null}function $C(e){return Array.isArray(e)}function RF(e){return typeof e=="string"}function kF(e){return typeof e=="boolean"}function NF(e){return typeof e=="number"}function zC(e){return typeof e=="object"&&e!==null}function OF(e){return zC(e)&&!$C(e)}function FF(e){if(!OF(e))return!1;let n=Object.getPrototypeOf(e);return n===Object.prototype||n===null}function Wg(e){return typeof e=="function"}function PF(e){return Wg(e)&&e.hasOwnProperty("\u0275cmp")}function LF(e,n){return Object.prototype.hasOwnProperty.call(e,n)}var jF=!1;function VF(){return jF}function PC(e,n){return e===n}function BF(e,n,t){for(let i=0;i<e.length;i++)if(!t(e[i],n[i]))return!0;return!1}function GC(e,n=PC,t=PC){let i=null,r=null,o;function s(){i=null,r=null}function a(d=void 0){o={result:d}}function c(){o=void 0}function l(){if(o!==void 0)return o.result;if(!i)return r=e.apply(null,arguments),i=arguments,r;if(!BF(arguments,i,n))return r;let d=e.apply(null,arguments);return i=arguments,t(r,d)?r:(r=d,d)}return{memoized:l,reset:s,setResult:a,clearResult:c}}function qa(...e){return HF(GC)(...e)}function UF(e,n,t,i){if(t===void 0){let o=n.map(s=>s(e));return i.memoized.apply(null,o)}let r=n.map(o=>o(e,t));return i.memoized.apply(null,[...r,t])}function HF(e,n={stateFn:UF}){return function(...t){let i=t;if(Array.isArray(i[0])){let[d,...u]=i;i=[...d,...u]}else i.length===1&&$F(i[0])&&(i=zF(i[0]));let r=i.slice(0,i.length-1),o=i[i.length-1],s=r.filter(d=>d.release&&typeof d.release=="function"),a=e(function(...d){return o.apply(null,d)}),c=GC(function(d,u){return n.stateFn.apply(null,[d,r,u,a])});function l(){c.reset(),a.reset(),s.forEach(d=>d.release())}return Object.assign(c.memoized,{release:l,projector:a.memoized,setResult:c.setResult,clearResult:c.clearResult})}}function WC(e){return qa(n=>{let t=n[e];return!VF()&&zm()&&!(e in n)&&console.warn(`@ngrx/store: The feature name "${e}" does not exist in the state, therefore createFeatureSelector cannot access it.  Be sure it is imported in a loaded module using StoreModule.forRoot('${e}', ...) or StoreModule.forFeature('${e}', ...).  If the default state is intended to be undefined, as is the case with router state, this development-only warning message can be ignored.`),t},n=>n)}function $F(e){return!!e&&typeof e=="object"&&Object.values(e).every(n=>typeof n=="function")}function zF(e){let n=Object.values(e),t=Object.keys(e),i=(...r)=>t.reduce((o,s,a)=>A(_({},o),{[s]:r[a]}),{});return[...n,i]}function GF(e){return e instanceof y?f(e):e}function qC(e){return typeof e=="function"?e():e}function WF(e,n){return e.concat(n)}function qF(){if(f(Lt,{optional:!0,skipSelf:!0}))throw new TypeError("The root Store has been provided more than once. Feature modules should provide feature states instead.");return"guarded"}function YF(e,n){return function(t,i){let r=n.action(i)?qg(i):i,o=e(t,r);return n.state()?qg(o):o}}function qg(e){Object.freeze(e);let n=Wg(e);return Object.getOwnPropertyNames(e).forEach(t=>{if(!t.startsWith("\u0275")&&LF(e,t)&&(!n||t!=="caller"&&t!=="callee"&&t!=="arguments")){let i=e[t];(zC(i)||Wg(i))&&!Object.isFrozen(i)&&qg(i)}}),e}function ZF(e,n){return function(t,i){if(n.action(i)){let o=Yg(i);LC(o,"action")}let r=e(t,i);if(n.state()){let o=Yg(r);LC(o,"state")}return r}}function Yg(e,n=[]){return(OC(e)||FC(e))&&n.length===0?{path:["root"],value:e}:Object.keys(e).reduce((i,r)=>{if(i)return i;let o=e[r];return PF(o)?i:OC(o)||FC(o)||NF(o)||kF(o)||RF(o)||$C(o)?!1:FF(o)?Yg(o,[...n,r]):{path:[...n,r],value:o}},!1)}function LC(e,n){if(e===!1)return;let t=e.path.join("."),i=new Error(`Detected unserializable ${n} at "${t}". ${Kg}#strict${n}serializability`);throw i.value=e.value,i.unserializablePath=t,i}function KF(e,n){return function(t,i){if(n.action(i)&&!j.isInAngularZone())throw new Error(`Action '${i.type}' running outside NgZone. ${Kg}#strictactionwithinngzone`);return e(t,i)}}function QF(e){return zm()?_({strictStateSerializability:!1,strictActionSerializability:!1,strictStateImmutability:!0,strictActionImmutability:!0,strictActionWithinNgZone:!1,strictActionTypeUniqueness:!1},e):{strictStateSerializability:!1,strictActionSerializability:!1,strictStateImmutability:!1,strictActionImmutability:!1,strictActionWithinNgZone:!1,strictActionTypeUniqueness:!1}}function XF({strictActionSerializability:e,strictStateSerializability:n}){return t=>e||n?ZF(t,{action:i=>e&&!Qg(i),state:()=>n}):t}function JF({strictActionImmutability:e,strictStateImmutability:n}){return t=>e||n?YF(t,{action:i=>e&&!Qg(i),state:()=>n}):t}function Qg(e){return e.type.startsWith("@ngrx")}function e1({strictActionWithinNgZone:e}){return n=>e?KF(n,{action:t=>e&&!Qg(t)}):n}function t1(e){return[{provide:kC,useValue:e},{provide:RC,useFactory:i1,deps:[kC]},{provide:$a,deps:[RC],useFactory:QF},{provide:pu,multi:!0,deps:[$a],useFactory:JF},{provide:pu,multi:!0,deps:[$a],useFactory:XF},{provide:pu,multi:!0,deps:[$a],useFactory:e1}]}function n1(){return[{provide:Zg,multi:!0,deps:[$a],useFactory:r1}]}function i1(e){return e}function r1(e){if(!e.strictActionTypeUniqueness)return;let n=Object.entries(Gg).filter(([,t])=>t>1).map(([t])=>t);if(n.length)throw new Error(`Action types are registered more than once, ${n.map(t=>`"${t}"`).join(", ")}. ${Kg}#strictactiontypeuniqueness`)}function o1(e={},n={}){return[{provide:jC,useFactory:qF},{provide:xC,useValue:n.initialState},{provide:Wa,useFactory:qC,deps:[xC]},{provide:zg,useValue:e},{provide:MC,useExisting:e instanceof y?e:zg},{provide:BC,deps:[zg,[new im(MC)]],useFactory:GF},{provide:TC,useValue:n.metaReducers?n.metaReducers:[]},{provide:AC,deps:[pu,TC],useFactory:WF},{provide:SC,useValue:n.reducerFactory?n.reducerFactory:wF},{provide:VC,deps:[SC,AC],useFactory:HC},yF,DF,IF,SF,MF,t1(n.runtimeChecks),n1()]}function s1(){f($n),f(Or),f(Fr),f(Lt),f(jC,{optional:!0}),f(Zg,{optional:!0})}var a1=[{provide:za,useFactory:s1},ro(()=>f(za))];function YC(e,n){return Tt([...o1(e,n),a1])}function c1(){f(za);let e=f(_F),n=f(bF),t=f(gu);f(Zg,{optional:!0});let i=e.map((r,o)=>{let a=n.shift()[o];return A(_({},r),{reducers:a,initialState:qC(r.initialState)})});t.addFeatures(i)}var bK=[{provide:mu,useFactory:c1},ro(()=>f(mu))];function zi(...e){let n=e.pop(),t=e.map(i=>i.type);return{reducer:n,types:t}}function ZC(e,...n){let t=new Map;for(let i of n)for(let r of i.types){let o=t.get(r);if(o){let s=(a,c)=>i.reducer(o(a,c),c);t.set(r,s)}else t.set(r,i.reducer)}return function(i=e,r){let o=t.get(r.type);return o?o(i,r):i}}var l1={dispatch:!0,functional:!1,useEffectsErrorHandler:!0},yu="__@ngrx/effects_create__";function _u(e,n={}){let t=n.functional?e:e(),i=_(_({},l1),n);return Object.defineProperty(t,yu,{value:i}),t}function d1(e){return Object.getOwnPropertyNames(e).filter(i=>e[i]&&e[i].hasOwnProperty(yu)?e[i][yu].hasOwnProperty("dispatch"):!1).map(i=>{let r=e[i][yu];return _({propertyName:i},r)})}function u1(e){return d1(e)}function KC(e){return Object.getPrototypeOf(e)}function f1(e){return!!e.constructor&&e.constructor.name!=="Object"&&e.constructor.name!=="Function"}function QC(e){return typeof e=="function"}function h1(e){return e.filter(QC)}function p1(e,n,t){let i=KC(e),o=!!i&&i.constructor.name!=="Object"?i.constructor.name:null,s=u1(e).map(({propertyName:a,dispatch:c,useEffectsErrorHandler:l})=>{let d=typeof e[a]=="function"?e[a]():e[a],u=l?t(d,n):d;return c===!1?u.pipe(Zf()):u.pipe(Qf()).pipe(T(p=>({effect:e[a],notification:p,propertyName:a,sourceName:o,sourceInstance:e})))});return jt(...s)}var m1=10;function XC(e,n,t=m1){return e.pipe(Et(i=>(n&&n.handleError(i),t<=1?e:XC(e,n,t-1))))}var JC=(()=>{class e extends H{constructor(t){super(),t&&(this.source=t)}lift(t){let i=new e;return i.source=this,i.operator=t,i}static{this.\u0275fac=function(i){return new(i||e)(I(Fr))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function bu(...e){return _e(n=>e.some(t=>typeof t=="string"?t===n.type:t.type===n.type))}var g1=new y("@ngrx/effects Effects Error Handler",{providedIn:"root",factory:()=>XC}),v1="@ngrx/effects/init",y1=Hn(v1);function _1(e,n){if(e.notification.kind==="N"){let t=e.notification.value;!b1(t)&&n.handleError(new Error(`Effect ${w1(e)} dispatched an invalid action: ${E1(t)}`))}}function b1(e){return typeof e!="function"&&e&&e.type&&typeof e.type=="string"}function w1({propertyName:e,sourceInstance:n,sourceName:t}){let i=typeof n[e]=="function";return!!t?`"${t}.${String(e)}${i?"()":""}"`:`"${String(e)}()"`}function E1(e){try{return JSON.stringify(e)}catch(n){return e}}var C1="ngrxOnIdentifyEffects";function D1(e){return Xg(e,C1)}var I1="ngrxOnRunEffects";function x1(e){return Xg(e,I1)}var S1="ngrxOnInitEffects";function M1(e){return Xg(e,S1)}function Xg(e,n){return e&&n in e&&typeof e[n]=="function"}var eD=(()=>{class e extends M{constructor(t,i){super(),this.errorHandler=t,this.effectsErrorHandler=i}addEffects(t){this.next(t)}toActions(){return this.pipe(Xc(t=>f1(t)?KC(t):t),Xe(t=>t.pipe(Xc(T1))),Xe(t=>{let i=t.pipe(Ei(o=>A1(this.errorHandler,this.effectsErrorHandler)(o)),T(o=>(_1(o,this.errorHandler),o.notification)),_e(o=>o.kind==="N"&&o.value!=null),Kf()),r=t.pipe(ct(1),_e(M1),T(o=>o.ngrxOnInitEffects()));return jt(i,r)}))}static{this.\u0275fac=function(i){return new(i||e)(I(Ye),I(g1))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function T1(e){return D1(e)?e.ngrxOnIdentifyEffects():""}function A1(e,n){return t=>{let i=p1(t,e,n);return x1(t)?t.ngrxOnRunEffects(i):i}}var R1=(()=>{class e{get isStarted(){return!!this.effectsSubscription}constructor(t,i){this.effectSources=t,this.store=i,this.effectsSubscription=null}start(){this.effectsSubscription||(this.effectsSubscription=this.effectSources.toActions().subscribe(this.store))}ngOnDestroy(){this.effectsSubscription&&(this.effectsSubscription.unsubscribe(),this.effectsSubscription=null)}static{this.\u0275fac=function(i){return new(i||e)(I(eD),I(Lt))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function tD(...e){let n=e.flat(),t=h1(n);return Tt([t,ro(()=>{f(za),f(mu,{optional:!0});let i=f(R1),r=f(eD),o=!i.isStarted;o&&i.start();for(let s of n){let a=QC(s)?f(s):s;r.addEffects(a)}o&&f(Lt).dispatch(y1())})])}var Ka="PERFORM_ACTION",k1="REFRESH",aD="RESET",cD="ROLLBACK",lD="COMMIT",dD="SWEEP",uD="TOGGLE_ACTION",N1="SET_ACTIONS_ACTIVE",fD="JUMP_TO_STATE",hD="JUMP_TO_ACTION",uv="IMPORT_STATE",pD="LOCK_CHANGES",mD="PAUSE_RECORDING",zo=class{constructor(n,t){if(this.action=n,this.timestamp=t,this.type=Ka,typeof n.type>"u")throw new Error('Actions may not have an undefined "type" property. Have you misspelled a constant?')}},Jg=class{constructor(){this.type=k1}},ev=class{constructor(n){this.timestamp=n,this.type=aD}},tv=class{constructor(n){this.timestamp=n,this.type=cD}},nv=class{constructor(n){this.timestamp=n,this.type=lD}},iv=class{constructor(){this.type=dD}},rv=class{constructor(n){this.id=n,this.type=uD}};var ov=class{constructor(n){this.index=n,this.type=fD}},sv=class{constructor(n){this.actionId=n,this.type=hD}},av=class{constructor(n){this.nextLiftedState=n,this.type=uv}},cv=class{constructor(n){this.status=n,this.type=pD}},lv=class{constructor(n){this.status=n,this.type=mD}};var Du=new y("@ngrx/store-devtools Options"),nD=new y("@ngrx/store-devtools Initial Config");function gD(){return null}var O1="NgRx Store DevTools";function F1(e){let n={maxAge:!1,monitor:gD,actionSanitizer:void 0,stateSanitizer:void 0,name:O1,serialize:!1,logOnly:!1,autoPause:!1,trace:!1,traceLimit:75,features:{pause:!0,lock:!0,persist:!0,export:!0,import:"custom",jump:!0,skip:!0,reorder:!0,dispatch:!0,test:!0},connectInZone:!1},t=typeof e=="function"?e():e,i=t.logOnly?{pause:!0,export:!0,test:!0}:!1,r=t.features||i||n.features;r.import===!0&&(r.import="custom");let o=Object.assign({},n,{features:r},t);if(o.maxAge&&o.maxAge<2)throw new Error(`Devtools 'maxAge' cannot be less than 2, got ${o.maxAge}`);return o}function iD(e,n){return e.filter(t=>n.indexOf(t)<0)}function vD(e){let{computedStates:n,currentStateIndex:t}=e;if(t>=n.length){let{state:r}=n[n.length-1];return r}let{state:i}=n[t];return i}function Za(e){return new zo(e,+Date.now())}function P1(e,n){return Object.keys(n).reduce((t,i)=>{let r=Number(i);return t[r]=yD(e,n[r],r),t},{})}function yD(e,n,t){return A(_({},n),{action:e(n.action,t)})}function L1(e,n){return n.map((t,i)=>({state:_D(e,t.state,i),error:t.error}))}function _D(e,n,t){return e(n,t)}function bD(e){return e.predicate||e.actionsSafelist||e.actionsBlocklist}function j1(e,n,t,i){let r=[],o={},s=[];return e.stagedActionIds.forEach((a,c)=>{let l=e.actionsById[a];l&&(c&&fv(e.computedStates[c],l,n,t,i)||(o[a]=l,r.push(a),s.push(e.computedStates[c])))}),A(_({},e),{stagedActionIds:r,actionsById:o,computedStates:s})}function fv(e,n,t,i,r){let o=t&&!t(e,n.action),s=i&&!n.action.type.match(i.map(c=>rD(c)).join("|")),a=r&&n.action.type.match(r.map(c=>rD(c)).join("|"));return o||s||a}function rD(e){return e.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")}function wD(e){return{ngZone:e?f(j):null,connectInZone:e}}var Iu=(()=>{class e extends $n{static{this.\u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})()}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})(),wu={START:"START",DISPATCH:"DISPATCH",STOP:"STOP",ACTION:"ACTION"},dv=new y("@ngrx/store-devtools Redux Devtools Extension"),ED=(()=>{class e{constructor(t,i,r){this.config=i,this.dispatcher=r,this.zoneConfig=wD(this.config.connectInZone),this.devtoolsExtension=t,this.createActionStreams()}notify(t,i){if(this.devtoolsExtension)if(t.type===Ka){if(i.isLocked||i.isPaused)return;let r=vD(i);if(bD(this.config)&&fv(r,t,this.config.predicate,this.config.actionsSafelist,this.config.actionsBlocklist))return;let o=this.config.stateSanitizer?_D(this.config.stateSanitizer,r,i.currentStateIndex):r,s=this.config.actionSanitizer?yD(this.config.actionSanitizer,t,i.nextActionId):t;this.sendToReduxDevtools(()=>this.extensionConnection.send(s,o))}else{let r=A(_({},i),{stagedActionIds:i.stagedActionIds,actionsById:this.config.actionSanitizer?P1(this.config.actionSanitizer,i.actionsById):i.actionsById,computedStates:this.config.stateSanitizer?L1(this.config.stateSanitizer,i.computedStates):i.computedStates});this.sendToReduxDevtools(()=>this.devtoolsExtension.send(null,r,this.getExtensionConfig(this.config)))}}createChangesObservable(){return this.devtoolsExtension?new H(t=>{let i=this.zoneConfig.connectInZone?this.zoneConfig.ngZone.runOutsideAngular(()=>this.devtoolsExtension.connect(this.getExtensionConfig(this.config))):this.devtoolsExtension.connect(this.getExtensionConfig(this.config));return this.extensionConnection=i,i.init(),i.subscribe(r=>t.next(r)),i.unsubscribe}):xe}createActionStreams(){let t=this.createChangesObservable().pipe(rr()),i=t.pipe(_e(l=>l.type===wu.START)),r=t.pipe(_e(l=>l.type===wu.STOP)),o=t.pipe(_e(l=>l.type===wu.DISPATCH),T(l=>this.unwrapAction(l.payload)),wi(l=>l.type===uv?this.dispatcher.pipe(_e(d=>d.type===vu),Yf(1e3),nr(1e3),T(()=>l),Et(()=>R(l)),ct(1)):R(l))),a=t.pipe(_e(l=>l.type===wu.ACTION),T(l=>this.unwrapAction(l.payload))).pipe(Ne(r)),c=o.pipe(Ne(r));this.start$=i.pipe(Ne(r)),this.actions$=this.start$.pipe(He(()=>a)),this.liftedActions$=this.start$.pipe(He(()=>c))}unwrapAction(t){return typeof t=="string"?(0,eval)(`(${t})`):t}getExtensionConfig(t){let i={name:t.name,features:t.features,serialize:t.serialize,autoPause:t.autoPause??!1,trace:t.trace??!1,traceLimit:t.traceLimit??75};return t.maxAge!==!1&&(i.maxAge=t.maxAge),i}sendToReduxDevtools(t){try{t()}catch(i){console.warn("@ngrx/store-devtools: something went wrong inside the redux devtools",i)}}static{this.\u0275fac=function(i){return new(i||e)(I(dv),I(Du),I(Iu))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})(),Cu={type:Ga},V1="@ngrx/store-devtools/recompute",B1={type:V1};function CD(e,n,t,i,r){if(i)return{state:t,error:"Interrupted by an error up the chain"};let o=t,s;try{o=e(t,n)}catch(a){s=a.toString(),r.handleError(a)}return{state:o,error:s}}function Eu(e,n,t,i,r,o,s,a,c){if(n>=e.length&&e.length===o.length)return e;let l=e.slice(0,n),d=o.length-(c?1:0);for(let u=n;u<d;u++){let h=o[u],p=r[h].action,m=l[u-1],b=m?m.state:i,D=m?m.error:void 0,L=s.indexOf(h)>-1?m:CD(t,p,b,D,a);l.push(L)}return c&&l.push(e[e.length-1]),l}function U1(e,n){return{monitorState:n(void 0,{}),nextActionId:1,actionsById:{0:Za(Cu)},stagedActionIds:[0],skippedActionIds:[],committedState:e,currentStateIndex:0,computedStates:[],isLocked:!1,isPaused:!1}}function H1(e,n,t,i,r={}){return o=>(s,a)=>{let{monitorState:c,actionsById:l,nextActionId:d,stagedActionIds:u,skippedActionIds:h,committedState:p,currentStateIndex:m,computedStates:b,isLocked:D,isPaused:S}=s||n;s||(l=Object.create(l));function L(fe){let he=fe,ne=u.slice(1,he+1);for(let sn=0;sn<ne.length;sn++)if(b[sn+1].error){he=sn,ne=u.slice(1,he+1);break}else delete l[ne[sn]];h=h.filter(sn=>ne.indexOf(sn)===-1),u=[0,...u.slice(he+1)],p=b[he].state,b=b.slice(he),m=m>he?m-he:0}function ue(){l={0:Za(Cu)},d=1,u=[0],h=[],p=b[m].state,m=0,b=[]}let G=0;switch(a.type){case pD:{D=a.status,G=1/0;break}case mD:{S=a.status,S?(u=[...u,d],l[d]=new zo({type:"@ngrx/devtools/pause"},+Date.now()),d++,G=u.length-1,b=b.concat(b[b.length-1]),m===u.length-2&&m++,G=1/0):ue();break}case aD:{l={0:Za(Cu)},d=1,u=[0],h=[],p=e,m=0,b=[];break}case lD:{ue();break}case cD:{l={0:Za(Cu)},d=1,u=[0],h=[],m=0,b=[];break}case uD:{let{id:fe}=a;h.indexOf(fe)===-1?h=[fe,...h]:h=h.filter(ne=>ne!==fe),G=u.indexOf(fe);break}case N1:{let{start:fe,end:he,active:ne}=a,sn=[];for(let Tf=fe;Tf<he;Tf++)sn.push(Tf);ne?h=iD(h,sn):h=[...h,...sn],G=u.indexOf(fe);break}case fD:{m=a.index,G=1/0;break}case hD:{let fe=u.indexOf(a.actionId);fe!==-1&&(m=fe),G=1/0;break}case dD:{u=iD(u,h),h=[],m=Math.min(m,u.length-1);break}case Ka:{if(D)return s||n;if(S||s&&fv(s.computedStates[m],a,r.predicate,r.actionsSafelist,r.actionsBlocklist)){let he=b[b.length-1];b=[...b.slice(0,-1),CD(o,a.action,he.state,he.error,t)],G=1/0;break}r.maxAge&&u.length===r.maxAge&&L(1),m===u.length-1&&m++;let fe=d++;l[fe]=a,u=[...u,fe],G=u.length-1;break}case uv:{({monitorState:c,actionsById:l,nextActionId:d,stagedActionIds:u,skippedActionIds:h,committedState:p,currentStateIndex:m,computedStates:b,isLocked:D,isPaused:S}=a.nextLiftedState);break}case Ga:{G=0,r.maxAge&&u.length>r.maxAge&&(b=Eu(b,G,o,p,l,u,h,t,S),L(u.length-r.maxAge),G=1/0);break}case vu:{if(b.filter(he=>he.error).length>0)G=0,r.maxAge&&u.length>r.maxAge&&(b=Eu(b,G,o,p,l,u,h,t,S),L(u.length-r.maxAge),G=1/0);else{if(!S&&!D){m===u.length-1&&m++;let he=d++;l[he]=new zo(a,+Date.now()),u=[...u,he],G=u.length-1,b=Eu(b,G,o,p,l,u,h,t,S)}b=b.map(he=>A(_({},he),{state:o(he.state,B1)})),m=u.length-1,r.maxAge&&u.length>r.maxAge&&L(u.length-r.maxAge),G=1/0}break}default:{G=1/0;break}}return b=Eu(b,G,o,p,l,u,h,t,S),c=i(c,a),{monitorState:c,actionsById:l,nextActionId:d,stagedActionIds:u,skippedActionIds:h,committedState:p,currentStateIndex:m,computedStates:b,isLocked:D,isPaused:S}}}var oD=(()=>{class e{constructor(t,i,r,o,s,a,c,l){let d=U1(c,l.monitor),u=H1(c,d,a,l.monitor,l),h=jt(jt(i.asObservable().pipe(or(1)),o.actions$).pipe(T(Za)),t,o.liftedActions$).pipe(Wn(ps)),p=r.pipe(T(u)),m=wD(l.connectInZone),b=new Xi(1);this.liftedStateSubscription=h.pipe(bs(p),sD(m),_s(({state:L},[ue,G])=>{let fe=G(L,ue);return ue.type!==Ka&&bD(l)&&(fe=j1(fe,l.predicate,l.actionsSafelist,l.actionsBlocklist)),o.notify(ue,fe),{state:fe,action:ue}},{state:d,action:null})).subscribe(({state:L,action:ue})=>{if(b.next(L),ue.type===Ka){let G=ue.action;s.next(G)}}),this.extensionStartSubscription=o.start$.pipe(sD(m)).subscribe(()=>{this.refresh()});let D=b.asObservable(),S=D.pipe(T(vD));Object.defineProperty(S,"state",{value:hu(S,{manualCleanup:!0,requireSync:!0})}),this.dispatcher=t,this.liftedState=D,this.state=S}ngOnDestroy(){this.liftedStateSubscription.unsubscribe(),this.extensionStartSubscription.unsubscribe()}dispatch(t){this.dispatcher.next(t)}next(t){this.dispatcher.next(t)}error(t){}complete(){}performAction(t){this.dispatch(new zo(t,+Date.now()))}refresh(){this.dispatch(new Jg)}reset(){this.dispatch(new ev(+Date.now()))}rollback(){this.dispatch(new tv(+Date.now()))}commit(){this.dispatch(new nv(+Date.now()))}sweep(){this.dispatch(new iv)}toggleAction(t){this.dispatch(new rv(t))}jumpToAction(t){this.dispatch(new sv(t))}jumpToState(t){this.dispatch(new ov(t))}importState(t){this.dispatch(new av(t))}lockChanges(t){this.dispatch(new cv(t))}pauseRecording(t){this.dispatch(new lv(t))}static{this.\u0275fac=function(i){return new(i||e)(I(Iu),I($n),I(Or),I(ED),I(Fr),I(Ye),I(Wa),I(Du))}}static{this.\u0275prov=E({token:e,factory:e.\u0275fac})}}return e})();function sD({ngZone:e,connectInZone:n}){return t=>n?new H(i=>t.subscribe({next:r=>e.run(()=>i.next(r)),error:r=>e.run(()=>i.error(r)),complete:()=>e.run(()=>i.complete())})):t}var $1=new y("@ngrx/store-devtools Is Devtools Extension or Monitor Present");function z1(e,n){return!!e||n.monitor!==gD}function G1(){let e="__REDUX_DEVTOOLS_EXTENSION__";return typeof window=="object"&&typeof window[e]<"u"?window[e]:null}function W1(e){return e.state}function DD(e={}){return Tt([ED,Iu,oD,{provide:nD,useValue:e},{provide:$1,deps:[dv,Du],useFactory:z1},{provide:dv,useFactory:G1},{provide:Du,deps:[nD],useFactory:F1},{provide:Ho,deps:[oD],useFactory:W1},{provide:Uo,useExisting:Iu}])}var Go=Hn("[Launch List] Load Launches"),Qa=Hn("[Launch List] Load Success",$o()),Wo=Hn("[Launch List] Load Failure",$o()),Pr=Hn("[Launch] Load Launch Details",$o()),qo=Hn("[Launch List] Load Favorite Launches"),Yo=Hn("[Launch] Add to Favorites",$o()),Zo=Hn("[Launch] Remove From Favorites",$o());var q1={launches:[],favoriteIds:[],loading:!1,error:null},xD=ZC(q1,zi(Go,e=>A(_({},e),{loading:!0})),zi(Qa,(e,{launches:n})=>A(_({},e),{loading:!1,launches:n})),zi(Wo,(e,{error:n})=>A(_({},e),{loading:!1,error:n})),zi(Pr,e=>A(_({},e),{loading:!0})),zi(qo,e=>{if(!Array.from(e.favoriteIds).length){let t=localStorage.getItem("spacex_app_favoriteIds");if(t){let i=JSON.parse(t),r=i.length?i:[];if(r.length)return A(_({},e),{favoriteIds:r})}}return _({},e)}),zi(Yo,(e,{launchId:n})=>{let t=Array.from(e.favoriteIds);return t.includes(n)||t.push(n),localStorage.setItem("spacex_app_favoriteIds",JSON.stringify(t)),A(_({},e),{favoriteIds:t})}),zi(Zo,(e,{launchId:n})=>{let t=Array.from(e.favoriteIds);if(t.includes(n)){let i=t.indexOf(n);t.splice(i,1)}return localStorage.setItem("spacex_app_favoriteIds",JSON.stringify(t)),A(_({},e),{favoriteIds:t})}));function Xa(e){return e.buttons===0||e.detail===0}function Ja(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var hv;function SD(){if(hv==null){let e=typeof document<"u"?document.head:null;hv=!!(e&&(e.createShadowRoot||e.attachShadow))}return hv}function pv(e){if(SD()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function kt(e){return e.composedPath?e.composedPath()[0]:e.target}var mv;try{mv=typeof Intl<"u"&&Intl.v8BreakIterator}catch(e){mv=!1}var We=(()=>{class e{_platformId=f(Cr);isBrowser=this._platformId?oE(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||mv)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ec;function MD(){if(ec==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>ec=!0}))}finally{ec=ec||!1}return ec}function Ko(e){return MD()?e:!!e.capture}function Qo(e,n=0){return TD(e)?Number(e):arguments.length===2?n:0}function TD(e){return!isNaN(parseFloat(e))&&!isNaN(Number(e))}function Dn(e){return e instanceof Q?e.nativeElement:e}var AD=new y("cdk-input-modality-detector-options"),RD={ignoreKeys:[18,17,224,91,16]},kD=650,gv={passive:!0,capture:!0},ND=(()=>{class e{_platform=f(We);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Pe(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(i=>i===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=kt(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<kD||(this._modality.next(Xa(t)?"keyboard":"mouse"),this._mostRecentTarget=kt(t))};_onTouchstart=t=>{if(Ja(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=kt(t)};constructor(){let t=f(j),i=f(W),r=f(AD,{optional:!0});if(this._options=_(_({},RD),r),this.modalityDetected=this._modality.pipe(or(1)),this.modalityChanged=this.modalityDetected.pipe(ys()),this._platform.isBrowser){let o=f(ut).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,gv),o.listen(i,"mousedown",this._onMousedown,gv),o.listen(i,"touchstart",this._onTouchstart,gv)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),tc=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(tc||{}),OD=new y("cdk-focus-monitor-default-options"),xu=Ko({passive:!0,capture:!0}),Lr=(()=>{class e{_ngZone=f(j);_platform=f(We);_inputModalityDetector=f(ND);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=f(W);_stopInputModalityDetector=new M;constructor(){let t=f(OD,{optional:!0});this._detectionMode=t?.detectionMode||tc.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let i=kt(t);for(let r=i;r;r=r.parentElement)t.type==="focus"?this._onFocus(t,r):this._onBlur(t,r)};monitor(t,i=!1){let r=Dn(t);if(!this._platform.isBrowser||r.nodeType!==1)return R();let o=pv(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new M,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(t){let i=Dn(t),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(t,i,r){let o=Dn(t),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((t,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===tc.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,i){t.classList.toggle("cdk-focused",!!i),t.classList.toggle("cdk-touch-focused",i==="touch"),t.classList.toggle("cdk-keyboard-focused",i==="keyboard"),t.classList.toggle("cdk-mouse-focused",i==="mouse"),t.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(t,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&i,this._detectionMode===tc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?kD:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(t,i){let r=this._elementInfo.get(i),o=kt(t);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(t,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&t.relatedTarget instanceof Node&&i.contains(t.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(t,i){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(i))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let i=t.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,xu),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,xu)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ne(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(t){let i=t.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,xu),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,xu),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,i,r){this._setClasses(t,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(t){let i=[];return this._elementInfo.forEach((r,o)=>{(o===t||r.checkChildren&&o.contains(t))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let o=t.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Su=new WeakMap,St=(()=>{class e{_appRef;_injector=f(re);_environmentInjector=f(Ee);load(t){let i=this._appRef=this._appRef||this._injector.get(Kt),r=Su.get(i);r||(r={loaders:new Set,refs:[]},Su.set(i,r),i.onDestroy(()=>{Su.get(i)?.refs.forEach(o=>o.destroy()),Su.delete(i)})),r.loaders.has(t)||(r.loaders.add(t),r.refs.push(Cd(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var vv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})(),Mu;function Y1(){if(Mu===void 0&&(Mu=null,typeof window<"u")){let e=window;e.trustedTypes!==void 0&&(Mu=e.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Mu}function jr(e){return Y1()?.createHTML(e)||e}function FD(e,n,t){let i=t.sanitize(nt.HTML,n);e.innerHTML=jr(i||"")}function nc(e){return Array.isArray(e)?e:[e]}var PD=new Set,Vr,Tu=(()=>{class e{_platform=f(We);_nonce=f(Dr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):K1}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&Z1(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Z1(e,n){if(!PD.has(e))try{Vr||(Vr=document.createElement("style"),n&&Vr.setAttribute("nonce",n),Vr.setAttribute("type","text/css"),document.head.appendChild(Vr)),Vr.sheet&&(Vr.sheet.insertRule(`@media ${e} {body{ }}`,0),PD.add(e))}catch(t){console.error(t)}}function K1(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var yv=(()=>{class e{_mediaMatcher=f(Tu);_zone=f(j);_queries=new Map;_destroySubject=new M;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(t){return LD(nc(t)).some(r=>this._registerQuery(r).mql.matches)}observe(t){let r=LD(nc(t)).map(s=>this._registerQuery(s).observable),o=ms(r);return o=bi(o.pipe(ct(1)),o.pipe(or(1),nr(0))),o.pipe(T(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(t){if(this._queries.has(t))return this._queries.get(t);let i=this._mediaMatcher.matchMedia(t),o={observable:new H(s=>{let a=c=>this._zone.run(()=>s.next(c));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(cn(i),T(({matches:s})=>({query:t,matches:s})),Ne(this._destroySubject)),mql:i};return this._queries.set(t,o),o}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function LD(e){return e.map(n=>n.split(",")).reduce((n,t)=>n.concat(t)).map(n=>n.trim())}var Q1=(()=>{class e{create(t){return typeof MutationObserver>"u"?null:new MutationObserver(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var jD=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({providers:[Q1]})}return e})();var VD=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),BD=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),X1=0,_v=(()=>{class e{_ngZone=f(j);_defaultOptions=f(BD,{optional:!0});_liveElement;_document=f(W);_sanitizer=f(Ca);_previousTimeout;_currentPromise;_currentResolve;constructor(){let t=f(VD,{optional:!0});this._liveElement=t||this._createLiveElement()}announce(t,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!t||typeof t=="string"?this._liveElement.textContent=t:FD(this._liveElement,t,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let t="cdk-live-announcer-element",i=this._document.getElementsByClassName(t),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(t),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${X1++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(t){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(t)===-1&&o.setAttribute("aria-owns",s+" "+t):o.setAttribute("aria-owns",t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var J1=200,Au=class{_letterKeyStream=new M;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new M;selectedItem=this._selectedItem;constructor(n,t){let i=typeof t?.debounceInterval=="number"?t.debounceInterval:J1;t?.skipPredicate&&(this._skipPredicateFn=t.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let t=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(t>=65&&t<=90||t>=48&&t<=57)&&this._letterKeyStream.next(String.fromCharCode(t))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Je(t=>this._pressedLetters.push(t)),nr(n),_e(()=>this._pressedLetters.length>0),T(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(t=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(t)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function UD(e,...n){return n.length?n.some(t=>e[t]):e.altKey||e.shiftKey||e.ctrlKey||e.metaKey}var Ru=class{_items;_activeItemIndex=Te(-1);_activeItem=Te(null);_wrap=!1;_typeaheadSubscription=Ie.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,t){this._items=n,n instanceof ni?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Li(n)&&(this._effectRef=hn(()=>this._itemsChanged(n()),{injector:t}))}tabOut=new M;change=new M;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let t=this._getItemsArray();return this._typeahead=new Au(t,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,t=10){return this._pageUpAndDown={enabled:n,delta:t},this}setActiveItem(n){let t=this._activeItem();this.updateActiveItem(n),this._activeItem()!==t&&this.change.next(this._activeItemIndex())}onKeydown(n){let t=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(t){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||UD(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let t=this._getItemsArray(),i=typeof n=="number"?n:t.indexOf(n),r=t[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let t=this._getItemsArray();for(let i=1;i<=t.length;i++){let r=(this._activeItemIndex()+n*i+t.length)%t.length,o=t[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,t){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=t,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Li(this._items)?this._items():this._items instanceof ni?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let t=this._activeItem();if(t){let i=n.indexOf(t);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var ic=class extends Ru{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var bv={},bt=class e{_appId=f(wo);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){return this._appId!=="ng"&&(n+=this._appId),bv.hasOwnProperty(n)||(bv[n]=0),`${n}${t?e._infix+"-":""}${bv[n]++}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})};var HD={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};function wv(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function Br(e){return e==null?"":typeof e=="string"?e:`${e}px`}var eP=new y("cdk-dir-doc",{providedIn:"root",factory:()=>f(W)}),tP=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function $D(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?tP.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var hi=(()=>{class e{get value(){return this.valueSignal()}valueSignal=Te("ltr");change=new oe;constructor(){let t=f(eP,{optional:!0});if(t){let i=t.body?t.body.dir:null,r=t.documentElement?t.documentElement.dir:null;this.valueSignal.set($D(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Ue=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({})}return e})();var rc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Xo=class extends rc{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,t,i,r,o){super(),this.component=n,this.viewContainerRef=t,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Jo=class extends rc{templateRef;viewContainerRef;context;injector;constructor(n,t,i,r){super(),this.templateRef=n,this.viewContainerRef=t,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,t=this.context){return this.context=t,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Ev=class extends rc{element;constructor(n){super(),this.element=n instanceof Q?n.nativeElement:n}},es=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Xo)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Jo)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Ev)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},ku=class extends es{outletElement;_appRef;_defaultInjector;constructor(n,t,i){super(),this.outletElement=n,this._appRef=t,this._defaultInjector=i}attachComponentPortal(n){let t;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Fn,null,{optional:!0})||void 0;t=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>t.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||re.NULL,o=r.get(Ee,i.injector);t=Cd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(t.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(t.hostView),t.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(t)),this._attachedPortal=n,t}attachTemplatePortal(n){let t=n.viewContainerRef,i=t.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=t.indexOf(i);r!==-1&&t.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let t=n.element;t.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");t.parentNode.insertBefore(i,t),this.outletElement.appendChild(t),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(t,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Cv=(()=>{class e extends es{_moduleRef=f(Fn,{optional:!0});_document=f(W);_viewContainerRef=f(yn);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(t){this.hasAttached()&&!t&&!this._isInitialized||(this.hasAttached()&&super.detach(),t&&super.attach(t),this._attachedPortal=t||null)}attached=new oe;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(t){t.setAttachedHost(this);let i=t.viewContainerRef!=null?t.viewContainerRef:this._viewContainerRef,r=i.createComponent(t.component,{index:i.length,injector:t.injector||i.injector,projectableNodes:t.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:t.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=t,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(t){t.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(t.templateRef,t.context,{injector:t.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=t,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=t=>{let i=t.element;i.parentNode;let r=this._document.createComment("dom-portal");t.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=t,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let t=this._viewContainerRef.element.nativeElement;return t.nodeType===t.ELEMENT_NODE?t:t.parentNode}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Qe]})}return e})();var Nu=class{enable(){}disable(){}attach(){}};var ts=class{positionStrategy;scrollStrategy=new Nu;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let t=Object.keys(n);for(let i of t)n[i]!==void 0&&(this[i]=n[i])}}};var WD=(()=>{class e{_attachedOverlays=[];_document=f(W);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(t){this.remove(t),this._attachedOverlays.push(t)}remove(t){let i=this._attachedOverlays.indexOf(t);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(t,i,r){return r.observers.length<1?!1:t.eventPredicate?t.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),qD=(()=>{class e extends WD{_ngZone=f(j);_renderer=f(ut).createRenderer(null,null);_cleanupKeydown;add(t){super.add(t),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=t=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,t,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(t));break}}};static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),YD=(()=>{class e extends WD{_platform=f(We);_ngZone=f(j);_renderer=f(ut).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(t){if(super.add(t),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(t=>t()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=t=>{this._pointerDownEventTarget=kt(t)};_clickListener=t=>{let i=kt(t),r=t.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,t,c))){if(zD(a.overlayElement,i)||zD(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(t)):c.next(t)}}};static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function zD(e,n){let t=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===e)return!0;i=t&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var ZD=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return e})(),iP=(()=>{class e{_platform=f(We);_containerElement;_document=f(W);_styleLoader=f(St);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let t="cdk-overlay-container";if(this._platform.isBrowser||wv()){let r=this._document.querySelectorAll(`.${t}[platform="server"], .${t}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(t),wv()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(ZD)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Dv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,t,i,r){this._renderer=t,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=t.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function KD(e){return e&&e.nodeType===1}var Ou=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new M;_attachments=new M;_detachments=new M;_positionStrategy;_scrollStrategy;_locationChanges=Ie.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new M;_outsidePointerEvents=new M;_afterNextRenderRef;constructor(n,t,i,r,o,s,a,c,l,d=!1,u,h){this._portalOutlet=n,this._host=t,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=u,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let t=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Zt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof t?.onDestroy=="function"&&t.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),t}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=_(_({},this._config),n),this._updateElementSize()}setDirection(n){this._config=A(_({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Br(this._config.width),n.height=Br(this._config.height),n.minWidth=Br(this._config.minWidth),n.minHeight=Br(this._config.minHeight),n.maxWidth=Br(this._config.maxWidth),n.maxHeight=Br(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;KD(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch(n){}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Dv(this._document,this._renderer,this._ngZone,t=>{this._backdropClick.next(t)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,t,i){let r=nc(t||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Zt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(t){if(n)throw t;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}};var GD="cdk-global-overlay-wrapper";function Iv(e){return new Fu}var Fu=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let t=n.getConfig();this._overlayRef=n,this._width&&!t.width&&n.updateSize({width:this._width}),this._height&&!t.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(GD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,u=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",p="",m="",b="";c?b="flex-start":d==="center"?(b="center",h?m=u:p=u):h?d==="left"||d==="end"?(b="flex-end",p=u):(d==="right"||d==="start")&&(b="flex-start",m=u):d==="left"||d==="start"?(b="flex-start",p=u):(d==="right"||d==="end")&&(b="flex-end",m=u),n.position=this._cssPosition,n.marginLeft=c?"0":p,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":m,t.justifyContent=b,t.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement,i=t.style;t.classList.remove(GD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}};var QD=new y("OVERLAY_DEFAULT_CONFIG");function xv(e,n){e.get(St).load(ZD);let t=e.get(iP),i=e.get(W),r=e.get(bt),o=e.get(Kt),s=e.get(hi),a=e.get(it,null,{optional:!0})||e.get(ut).createRenderer(null,null),c=new ts(n),l=e.get(QD,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),u=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),u.appendChild(d),c.usePopover&&(u.setAttribute("popover","manual"),u.classList.add("cdk-overlay-popover"));let h=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return KD(h)?h.after(u):h?.type==="parent"?h.element.appendChild(u):t.getContainerElement().appendChild(u),new Ou(new ku(d,o,e),u,d,c,e.get(j),e.get(qD),i,e.get(Vi),e.get(YD),n?.disableAnimations??e.get(Zs,null,{optional:!0})==="NoopAnimations",e.get(Ee),a)}var ns,XD=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Sv(){if(ns)return ns;if(typeof document!="object"||!document)return ns=new Set(XD),ns;let e=document.createElement("input");return ns=new Set(XD.filter(n=>(e.setAttribute("type",n),e.type===n))),ns}var oP=new y("MATERIAL_ANIMATIONS"),JD=null;function Mv(){return f(oP,{optional:!0})?.animationsDisabled||f(Zs,{optional:!0})==="NoopAnimations"?"di-disabled":(JD??=f(Tu).matchMedia("(prefers-reduced-motion)").matches,JD?"reduced-motion":"enabled")}function Mt(){return Mv()!=="enabled"}function is(e){return e!=null&&`${e}`!="false"}var on=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(on||{}),Tv=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=on.HIDDEN;constructor(n,t,i,r=!1){this._renderer=n,this.element=t,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},eI=Ko({passive:!0,capture:!0}),Av=class{_events=new Map;addHandler(n,t,i,r){let o=this._events.get(t);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(t,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,eI)})}removeHandler(n,t,i){let r=this._events.get(n);if(!r)return;let o=r.get(t);o&&(o.delete(i),o.size===0&&r.delete(t),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,eI)))}_delegateEventHandler=n=>{let t=kt(n);t&&this._events.get(n.type)?.forEach((i,r)=>{(r===t||r.contains(t))&&i.forEach(o=>o.handleEvent(n))})}},oc={enterDuration:225,exitDuration:150},sP=800,tI=Ko({passive:!0,capture:!0}),nI=["mousedown","touchstart"],iI=["mouseup","mouseleave","touchend","touchcancel"],aP=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return e})(),sc=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Av;constructor(n,t,i,r,o){this._target=n,this._ngZone=t,this._platform=r,r.isBrowser&&(this._containerElement=Dn(i)),o&&o.get(St).load(aP)}fadeInRipple(n,t,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=_(_({},oc),i.animation);i.centered&&(n=r.left+r.width/2,t=r.top+r.height/2);let s=i.radius||cP(n,t,r),a=n-r.left,c=t-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),h=u.transitionProperty,p=u.transitionDuration,m=h==="none"||p==="0s"||p==="0s, 0s"||r.width===0&&r.height===0,b=new Tv(this,d,i,m);d.style.transform="scale3d(1, 1, 1)",b.state=on.FADING_IN,i.persistent||(this._mostRecentTransientRipple=b);let D=null;return!m&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let S=()=>{D&&(D.fallbackTimer=null),clearTimeout(ue),this._finishRippleTransition(b)},L=()=>this._destroyRipple(b),ue=setTimeout(L,l+100);d.addEventListener("transitionend",S),d.addEventListener("transitioncancel",L),D={onTransitionEnd:S,onTransitionCancel:L,fallbackTimer:ue}}),this._activeRipples.set(b,D),(m||!l)&&this._finishRippleTransition(b),b}fadeOutRipple(n){if(n.state===on.FADING_OUT||n.state===on.HIDDEN)return;let t=n.element,i=_(_({},oc),n.config.animation);t.style.transitionDuration=`${i.exitDuration}ms`,t.style.opacity="0",n.state=on.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=Dn(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,nI.forEach(i=>{e._eventManager.addHandler(this._ngZone,i,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{iI.forEach(t=>{this._triggerElement.addEventListener(t,this,tI)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===on.FADING_IN?this._startFadeOutTransition(n):n.state===on.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=on.VISIBLE,!i&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=on.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=Xa(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+sP;!this._target.rippleDisabled&&!t&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Ja(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let i=0;i<t.length;i++)this.fadeInRipple(t[i].clientX,t[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===on.VISIBLE||n.config.terminateOnPointerUp&&n.state===on.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(nI.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(iI.forEach(t=>n.removeEventListener(t,this,tI)),this._pointerUpEventsRegistered=!1))}};function cP(e,n,t){let i=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),r=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(i*i+r*r)}var ac=new y("mat-ripple-global-options"),rI=(()=>{class e{_elementRef=f(Q);_animationsDisabled=Mt();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(t){t&&this.fadeOutAllNonPersistent(),this._disabled=t,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(t){this._trigger=t,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let t=f(j),i=f(We),r=f(ac,{optional:!0}),o=f(re);this._globalOptions=r||{},this._rippleRenderer=new sc(this,t,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:_(_(_({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(t,i=0,r){return typeof t=="number"?this._rippleRenderer.fadeInRipple(t,i,_(_({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,_(_({},this.rippleConfig),t))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&te("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return e})();var lP={capture:!0},dP=["focus","mousedown","mouseenter","touchstart"],Rv="mat-ripple-loader-uninitialized",kv="mat-ripple-loader-class-name",oI="mat-ripple-loader-centered",Pu="mat-ripple-loader-disabled",Lu=(()=>{class e{_document=f(W);_animationsDisabled=Mt();_globalRippleOptions=f(ac,{optional:!0});_platform=f(We);_ngZone=f(j);_injector=f(re);_eventCleanups;_hosts=new Map;constructor(){let t=f(ut).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>dP.map(i=>t.listen(this._document,i,this._onInteraction,lP)))}ngOnDestroy(){let t=this._hosts.keys();for(let i of t)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(t,i){t.setAttribute(Rv,this._globalRippleOptions?.namespace??""),(i.className||!t.hasAttribute(kv))&&t.setAttribute(kv,i.className||""),i.centered&&t.setAttribute(oI,""),i.disabled&&t.setAttribute(Pu,"")}setDisabled(t,i){let r=this._hosts.get(t);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(t))):i?t.setAttribute(Pu,""):t.removeAttribute(Pu)}_onInteraction=t=>{let i=kt(t);if(i instanceof HTMLElement){let r=i.closest(`[${Rv}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",t.getAttribute(kv)),t.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??oc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??oc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||t.hasAttribute(Pu),rippleConfig:{centered:t.hasAttribute(oI),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new sc(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:a,renderer:c,hasSetUpEvents:l}),t.removeAttribute(Rv)}destroyRipple(t){let i=this._hosts.get(t);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Ur=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var uP=new y("MAT_BUTTON_CONFIG");function sI(e){return e==null?void 0:Vn(e)}var aI=(()=>{class e{_elementRef=f(Q);_ngZone=f(j);_animationsDisabled=Mt();_config=f(uP,{optional:!0});_focusMonitor=f(Lr);_cleanupClick;_renderer=f(it);_rippleLoader=f(Lu);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}constructor(){f(St).load(Ur);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",i){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(be("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),wn(r.color?"mat-"+r.color:""),te("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",we],disabled:[2,"disabled","disabled",we],ariaDisabled:[2,"aria-disabled","ariaDisabled",we],disabledInteractive:[2,"disabledInteractive","disabledInteractive",we],tabIndex:[2,"tabIndex","tabIndex",sI],_tabindex:[2,"tabindex","_tabindex",sI]}})}return e})();var ju=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[Ue]})}return e})();var fP=["matButton",""],hP=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],pP=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var cI=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),rs=(()=>{class e extends aI{get appearance(){return this._appearance}set appearance(t){this.setAppearance(t||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let t=mP(this._elementRef.nativeElement);t&&this.setAppearance(t)}setAppearance(t){if(t===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?cI.get(this._appearance):null,o=cI.get(t);r&&i.remove(...r),i.add(...o),this._appearance=t}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Qe],attrs:fP,ngContentSelectors:pP,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Be(hP),At(0,"span",0),se(1),rt(2,"span",1),se(3,1),ot(),se(4,2),At(5,"span",2)(6,"span",3)),i&2&&te("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();function mP(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var cc=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[ju,Ue]})}return e})();function gP(e,n){if(e&1){let t=_n();g(0,"div",1)(1,"button",2),pe("click",function(){zt(t);let r=q();return Gt(r.action())}),C(2),v()()}if(e&2){let t=q();w(2),Fe(" ",t.data.action," ")}}var vP=["label"];function yP(e,n){}var _P=Math.pow(2,31)-1,lc=class{_overlayRef;instance;containerInstance;_afterDismissed=new M;_afterOpened=new M;_onAction=new M;_durationTimeoutId;_dismissedByAction=!1;constructor(n,t){this._overlayRef=t,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,_P))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},dI=new y("MatSnackBarData"),os=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},bP=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return e})(),wP=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return e})(),EP=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return e})(),CP=(()=>{class e{snackBarRef=f(lc);data=f(dI);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(g(0,"div",0),C(1),v(),k(2,gP,3,1,"div",1)),i&2&&(w(),Fe(" ",r.data.message,`
`),w(),N(r.hasAction?2:-1))},dependencies:[rs,bP,wP,EP],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return e})(),Nv="_mat-snack-bar-enter",Ov="_mat-snack-bar-exit",DP=(()=>{class e extends es{_ngZone=f(j);_elementRef=f(Q);_changeDetectorRef=f(ht);_platform=f(We);_animationsDisabled=Mt();snackBarConfig=f(os);_document=f(W);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=f(re);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new M;_onExit=new M;_onEnter=new M;_animationState="void";_live;_label;_role;_liveElementId=f(bt).getId("mat-snack-bar-container-live-");constructor(){super();let t=this.snackBarConfig;t.politeness==="assertive"&&!t.announcementMessage?this._live="assertive":t.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(t){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(t);return this._afterPortalAttached(),i}attachTemplatePortal(t){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(t);return this._afterPortalAttached(),i}attachDomPortal=t=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(t);return this._afterPortalAttached(),i};onAnimationEnd(t){t===Ov?this._completeExit():t===Nv&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Zt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Nv)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Nv)},200)))}exit(){return this._destroyed?R(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Zt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Ov)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Ov),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let t=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>t.classList.add(s)):t.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let t=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(t)===-1&&o.setAttribute("aria-owns",s+" "+t):o.setAttribute("aria-owns",t)}}_clearFromModals(){this._trackedModals.forEach(t=>{let i=t.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?t.setAttribute("aria-owns",r):t.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let t=this._elementRef.nativeElement,i=t.querySelector("[aria-hidden]"),r=t.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&bn(Cv,7)(vP,7),i&2){let o;ce(o=le())&&(r._portalOutlet=o.first),ce(o=le())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&pe("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&te("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Qe],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(g(0,"div",1)(1,"div",2,0)(3,"div",3),Pi(4,yP,0,0,"ng-template",4),v(),Ce(5,"div"),v()()),i&2&&(w(5),be("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[Cv],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return e})(),IP=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new os}),uI=(()=>{class e{_live=f(_v);_injector=f(re);_breakpointObserver=f(yv);_parentSnackBar=f(e,{optional:!0,skipSelf:!0});_defaultConfig=f(IP);_animationsDisabled=Mt();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=CP;snackBarContainerComponent=DP;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let t=this._parentSnackBar;return t?t._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(t){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=t:this._snackBarRefAtThisLevel=t}constructor(){}openFromComponent(t,i){return this._attach(t,i)}openFromTemplate(t,i){return this._attach(t,i)}open(t,i="",r){let o=_(_({},this._defaultConfig),r);return o.data={message:t,action:i},o.announcementMessage===t&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(t,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=re.create({parent:r||this._injector,providers:[{provide:os,useValue:i}]}),s=new Xo(this.snackBarContainerComponent,i.viewContainerRef,o),a=t.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(t,i){let r=_(_(_({},new os),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new lc(s,o);if(t instanceof On){let c=new Jo(t,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(c)}else{let c=this._createInjector(r,a),l=new Xo(t,void 0,c),d=s.attachComponentPortal(l);a.instance=d.instance}return this._breakpointObserver.observe(HD.HandsetPortrait).pipe(Ne(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(t,i){t.afterDismissed().subscribe(()=>{this._openedSnackBarRef==t&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&t.afterOpened().subscribe(()=>t._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{t.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):t.containerInstance.enter()}_createOverlay(t){let i=new ts;i.direction=t.direction;let r=Iv(this._injector),o=t.direction==="rtl",s=t.horizontalPosition==="left"||t.horizontalPosition==="start"&&!o||t.horizontalPosition==="end"&&o,a=!s&&t.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),t.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,xv(this._injector,i)}_createInjector(t,i){let r=t&&t.viewContainerRef&&t.viewContainerRef.injector;return re.create({parent:r||this._injector,providers:[{provide:lc,useValue:i},{provide:dI,useValue:t.data}]})}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Vu=class e{constructor(n){this.http=n}apiUrl="https://api.spacexdata.com/v4/launches";snackBar=f(uI);getPastLaunches(){return this.http.get(`${this.apiUrl}/past`)}getLaunchById(n){return this.http.get(`${this.apiUrl}/${n}`)}openSnackBar(n,t){this.snackBar.open(n,t)}static \u0275fac=function(t){return new(t||e)(I(To))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})};var Bu=class e{spaceXService=f(Vu);actions$=f(JC);handleError(n){return n.error instanceof ProgressEvent?R(Wo({error:"Unknown API error"})):R(Wo({error:n.error}))}loadLaunches$=_u(()=>this.actions$.pipe(bu(Go),Ei(()=>this.spaceXService.getPastLaunches().pipe(T(n=>Qa({launches:n})),Et(n=>this.handleError(n))))));loadLaunchDetail$=_u(()=>this.actions$.pipe(bu(Pr),Ei(n=>this.spaceXService.getLaunchById(n.launchId).pipe(T(t=>Qa({launches:[t]})),Et(t=>this.handleError(t))))));loadLaunchesFailure$=_u(()=>this.actions$.pipe(bu(Wo),He(n=>(this.spaceXService.openSnackBar(n.error,"Error"),xe))),{dispatch:!1});static \u0275fac=function(t){return new(t||e)};static \u0275prov=E({token:e,factory:e.\u0275fac})};var _I=(()=>{class e{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||e)(ee(it),ee(Q))};static \u0275dir=B({type:e})}return e})(),xP=(()=>{class e extends _I{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275dir=B({type:e,features:[Qe]})}return e})(),Ju=new y("");var SP={provide:Ju,useExisting:Vt(()=>ef),multi:!0};function MP(){let e=en()?en().getUserAgent():"";return/android (\d+)/.test(e.toLowerCase())}var TP=new y(""),ef=(()=>{class e extends _I{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!MP())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||e)(ee(it),ee(Q),ee(TP,8))};static \u0275dir=B({type:e,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&pe("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[st([SP]),Qe]})}return e})();function Lv(e){return e==null||jv(e)===0}function jv(e){return e==null?null:Array.isArray(e)||typeof e=="string"?e.length:e instanceof Set?e.size:null}var mc=new y(""),Vv=new y(""),AP=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Hu=class{static min(n){return RP(n)}static max(n){return kP(n)}static required(n){return NP(n)}static requiredTrue(n){return OP(n)}static email(n){return FP(n)}static minLength(n){return PP(n)}static maxLength(n){return LP(n)}static pattern(n){return jP(n)}static nullValidator(n){return bI()}static compose(n){return xI(n)}static composeAsync(n){return SI(n)}};function RP(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t<e?{min:{min:e,actual:n.value}}:null}}function kP(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t>e?{max:{max:e,actual:n.value}}:null}}function NP(e){return Lv(e.value)?{required:!0}:null}function OP(e){return e.value===!0?null:{required:!0}}function FP(e){return Lv(e.value)||AP.test(e.value)?null:{email:!0}}function PP(e){return n=>{let t=n.value?.length??jv(n.value);return t===null||t===0?null:t<e?{minlength:{requiredLength:e,actualLength:t}}:null}}function LP(e){return n=>{let t=n.value?.length??jv(n.value);return t!==null&&t>e?{maxlength:{requiredLength:e,actualLength:t}}:null}}function jP(e){if(!e)return bI;let n,t;return typeof e=="string"?(t="",e.charAt(0)!=="^"&&(t+="^"),t+=e,e.charAt(e.length-1)!=="$"&&(t+="$"),n=new RegExp(t)):(t=e.toString(),n=e),i=>{if(Lv(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:t,actualValue:r}}}}function bI(e){return null}function wI(e){return e!=null}function EI(e){return oi(e)?ke(e):e}function CI(e){let n={};return e.forEach(t=>{n=t!=null?_(_({},n),t):n}),Object.keys(n).length===0?null:n}function DI(e,n){return n.map(t=>t(e))}function VP(e){return!e.validate}function II(e){return e.map(n=>VP(n)?n:t=>n.validate(t))}function xI(e){if(!e)return null;let n=e.filter(wI);return n.length==0?null:function(t){return CI(DI(t,n))}}function Bv(e){return e!=null?xI(II(e)):null}function SI(e){if(!e)return null;let n=e.filter(wI);return n.length==0?null:function(t){let i=DI(t,n).map(EI);return vs(i).pipe(T(CI))}}function Uv(e){return e!=null?SI(II(e)):null}function fI(e,n){return e===null?[n]:Array.isArray(e)?[...e,n]:[e,n]}function MI(e){return e._rawValidators}function TI(e){return e._rawAsyncValidators}function Fv(e){return e?Array.isArray(e)?e:[e]:[]}function $u(e,n){return Array.isArray(e)?e.includes(n):e===n}function hI(e,n){let t=Fv(n);return Fv(e).forEach(r=>{$u(t,r)||t.push(r)}),t}function pI(e,n){return Fv(n).filter(t=>!$u(e,t))}var zu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Bv(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Uv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,t){return this.control?this.control.hasError(n,t):!1}getError(n,t){return this.control?this.control.getError(n,t):null}},Hr=class extends zu{name;get formDirective(){return null}get path(){return null}},$r=class extends zu{_parent=null;name=null;valueAccessor=null},Pv=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var AI=(()=>{class e extends Pv{constructor(t){super(t)}static \u0275fac=function(i){return new(i||e)(ee($r,2))};static \u0275dir=B({type:e,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&te("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Qe]})}return e})();var dc="VALID",Uu="INVALID",ss="PENDING",uc="DISABLED",Gi=class{},Gu=class extends Gi{value;source;constructor(n,t){super(),this.value=n,this.source=t}},hc=class extends Gi{pristine;source;constructor(n,t){super(),this.pristine=n,this.source=t}},pc=class extends Gi{touched;source;constructor(n,t){super(),this.touched=n,this.source=t}},as=class extends Gi{status;source;constructor(n,t){super(),this.status=n,this.source=t}},Wu=class extends Gi{source;constructor(n){super(),this.source=n}},qu=class extends Gi{source;constructor(n){super(),this.source=n}};function RI(e){return(tf(e)?e.validators:e)||null}function BP(e){return Array.isArray(e)?Bv(e):e||null}function kI(e,n){return(tf(n)?n.asyncValidators:e)||null}function UP(e){return Array.isArray(e)?Uv(e):e||null}function tf(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}function HP(e,n,t){let i=e.controls;if(!(n?Object.keys(i):i).length)throw new x(1e3,"");if(!i[t])throw new x(1001,"")}function $P(e,n,t){e._forEachChild((i,r)=>{if(t[r]===void 0)throw new x(-1002,"")})}var Yu=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,t){this._assignValidators(n),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return je(this.statusReactive)}set status(n){je(()=>this.statusReactive.set(n))}_status=Ge(()=>this.statusReactive());statusReactive=Te(void 0);get valid(){return this.status===dc}get invalid(){return this.status===Uu}get pending(){return this.status===ss}get disabled(){return this.status===uc}get enabled(){return this.status!==uc}errors;get pristine(){return je(this.pristineReactive)}set pristine(n){je(()=>this.pristineReactive.set(n))}_pristine=Ge(()=>this.pristineReactive());pristineReactive=Te(!0);get dirty(){return!this.pristine}get touched(){return je(this.touchedReactive)}set touched(n){je(()=>this.touchedReactive.set(n))}_touched=Ge(()=>this.touchedReactive());touchedReactive=Te(!1);get untouched(){return!this.touched}_events=new M;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(hI(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(hI(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(pI(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(pI(n,this._rawAsyncValidators))}hasValidator(n){return $u(this._rawValidators,n)}hasAsyncValidator(n){return $u(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let t=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(A(_({},n),{sourceControl:i})),t&&n.emitEvent!==!1&&this._events.next(new pc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(n))}markAsUntouched(n={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),t&&n.emitEvent!==!1&&this._events.next(new pc(!1,i))}markAsDirty(n={}){let t=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(A(_({},n),{sourceControl:i})),t&&n.emitEvent!==!1&&this._events.next(new hc(!1,i))}markAsPristine(n={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),t&&n.emitEvent!==!1&&this._events.next(new hc(!0,i))}markAsPending(n={}){this.status=ss;let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new as(this.status,t)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(A(_({},n),{sourceControl:t}))}disable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=uc,this.errors=null,this._forEachChild(r=>{r.disable(A(_({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Gu(this.value,i)),this._events.next(new as(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(A(_({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=dc,this._forEachChild(i=>{i.enable(A(_({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(A(_({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,t){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===dc||this.status===ss)&&this._runAsyncValidator(i,n.emitEvent)}let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Gu(this.value,t)),this._events.next(new as(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(A(_({},n),{sourceControl:t}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?uc:dc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,t){if(this.asyncValidator){this.status=ss,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:n!==!1};let i=EI(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,t={}){this.errors=n,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(n){let t=n;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(n,t){let i=t?this.get(t):this;return i?.errors?i.errors[n]:null}hasError(n,t){return!!this.getError(n,t)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,t,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new as(this.status,t)),this._parent&&this._parent._updateControlsErrors(n,t,i)}_initObservables(){this.valueChanges=new oe,this.statusChanges=new oe}_calculateStatus(){return this._allControlsDisabled()?uc:this.errors?Uu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ss)?ss:this._anyControlsHaveStatus(Uu)?Uu:dc}_anyControlsHaveStatus(n){return this._anyControls(t=>t.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,t),r&&this._events.next(new hc(this.pristine,t))}_updateTouched(n={},t){this.touched=this._anyControlsTouched(),this._events.next(new pc(this.touched,t)),n.onlySelf||this._parent?._updateTouched(n,t)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){tf(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=BP(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=UP(this._rawAsyncValidators)}},Zu=class extends Yu{constructor(n,t,i){super(RI(t),kI(i,t)),this.controls=n,this._initObservables(),this._setUpdateStrategy(t),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,t){return this.controls[n]?this.controls[n]:(this.controls[n]=t,t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange),t)}addControl(n,t,i={}){this.registerControl(n,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,t={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:t.emitEvent}),this._onCollectionChange()}setControl(n,t,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],t&&this.registerControl(n,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,t={}){$P(this,!0,n),Object.keys(n).forEach(i=>{HP(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t)}patchValue(n,t={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t))}reset(n={},t={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,A(_({},t),{onlySelf:!0}))}),this._updatePristine(t,this),this._updateTouched(t,this),this.updateValueAndValidity(t),t?.emitEvent!==!1&&this._events.next(new qu(this))}getRawValue(){return this._reduceChildren({},(n,t,i)=>(n[i]=t.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(t,i)=>i._syncPendingControls()?!0:t);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(t=>{let i=this.controls[t];i&&n(i,t)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[t,i]of Object.entries(this.controls))if(this.contains(t)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(t,i,r)=>((i.enabled||this.disabled)&&(t[r]=i.value),t))}_reduceChildren(n,t){let i=n;return this._forEachChild((r,o)=>{i=t(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var nf=new y("",{factory:()=>Hv}),Hv="always";function zP(e,n){return[...n.path,e]}function Ku(e,n,t=Hv){$v(e,n),n.valueAccessor.writeValue(e.value),(e.disabled||t==="always")&&n.valueAccessor.setDisabledState?.(e.disabled),WP(e,n),YP(e,n),qP(e,n),GP(e,n)}function mI(e,n,t=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Xu(e,n),e&&(n._invokeOnDestroyCallbacks(),e._registerOnCollectionChange(()=>{}))}function Qu(e,n){e.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(n)})}function GP(e,n){if(n.valueAccessor.setDisabledState){let t=i=>{n.valueAccessor.setDisabledState(i)};e.registerOnDisabledChange(t),n._registerOnDestroy(()=>{e._unregisterOnDisabledChange(t)})}}function $v(e,n){let t=MI(e);n.validator!==null?e.setValidators(fI(t,n.validator)):typeof t=="function"&&e.setValidators([t]);let i=TI(e);n.asyncValidator!==null?e.setAsyncValidators(fI(i,n.asyncValidator)):typeof i=="function"&&e.setAsyncValidators([i]);let r=()=>e.updateValueAndValidity();Qu(n._rawValidators,r),Qu(n._rawAsyncValidators,r)}function Xu(e,n){let t=!1;if(e!==null){if(n.validator!==null){let r=MI(e);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(t=!0,e.setValidators(o))}}if(n.asyncValidator!==null){let r=TI(e);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(t=!0,e.setAsyncValidators(o))}}}let i=()=>{};return Qu(n._rawValidators,i),Qu(n._rawAsyncValidators,i),t}function WP(e,n){n.valueAccessor.registerOnChange(t=>{e._pendingValue=t,e._pendingChange=!0,e._pendingDirty=!0,e.updateOn==="change"&&NI(e,n)})}function qP(e,n){n.valueAccessor.registerOnTouched(()=>{e._pendingTouched=!0,e.updateOn==="blur"&&e._pendingChange&&NI(e,n),e.updateOn!=="submit"&&e.markAsTouched()})}function NI(e,n){e._pendingDirty&&e.markAsDirty(),e.setValue(e._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(e._pendingValue),e._pendingChange=!1}function YP(e,n){let t=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};e.registerOnChange(t),n._registerOnDestroy(()=>{e._unregisterOnChange(t)})}function OI(e,n){e==null,$v(e,n)}function ZP(e,n){return Xu(e,n)}function KP(e,n){if(!e.hasOwnProperty("model"))return!1;let t=e.model;return t.isFirstChange()?!0:!Object.is(n,t.currentValue)}function QP(e){return Object.getPrototypeOf(e.constructor)===xP}function FI(e,n){e._syncPendingControls(),n.forEach(t=>{let i=t.control;i.updateOn==="submit"&&i._pendingChange&&(t.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function XP(e,n){if(!n)return null;Array.isArray(n);let t,i,r;return n.forEach(o=>{o.constructor===ef?t=o:QP(o)?i=o:r=o}),r||i||t||null}function JP(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}var eL={provide:Hr,useExisting:Vt(()=>zv)},fc=Promise.resolve(),zv=(()=>{class e extends Hr{callSetDisabledState;get submitted(){return je(this.submittedReactive)}_submitted=Ge(()=>this.submittedReactive());submittedReactive=Te(!1);_directives=new Set;form;ngSubmit=new oe;options;constructor(t,i,r){super(),this.callSetDisabledState=r,this.form=new Zu({},Bv(t),Uv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(t){fc.then(()=>{let i=this._findContainer(t.path);t.control=i.registerControl(t.name,t.control),Ku(t.control,t,this.callSetDisabledState),t.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(t)})}getControl(t){return this.form.get(t.path)}removeControl(t){fc.then(()=>{this._findContainer(t.path)?.removeControl(t.name),this._directives.delete(t)})}addFormGroup(t){fc.then(()=>{let i=this._findContainer(t.path),r=new Zu({});OI(r,t),i.registerControl(t.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(t){fc.then(()=>{this._findContainer(t.path)?.removeControl?.(t.name)})}getFormGroup(t){return this.form.get(t.path)}updateModel(t,i){fc.then(()=>{this.form.get(t.path).setValue(i)})}setValue(t){this.control.setValue(t)}onSubmit(t){return this.submittedReactive.set(!0),FI(this.form,this._directives),this.ngSubmit.emit(t),this.form._events.next(new Wu(this.control)),t?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(t=void 0){this.form.reset(t),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(t){return t.pop(),t.length?this.form.get(t):this.form}static \u0275fac=function(i){return new(i||e)(ee(mc,10),ee(Vv,10),ee(nf,8))};static \u0275dir=B({type:e,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&pe("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[st([eL]),Qe]})}return e})();function gI(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function vI(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var PI=class extends Yu{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,t,i){super(RI(t),kI(i,t)),this._applyFormState(n),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),tf(t)&&(t.nonNullable||t.initialValueIsDefault)&&(vI(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,t={}){this.value=this._pendingValue=n,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(n,t={}){this.setValue(n,t)}reset(n=this.defaultValue,t={}){this._applyFormState(n),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new qu(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){gI(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){gI(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){vI(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var tL=e=>e instanceof PI;var nL={provide:$r,useExisting:Vt(()=>Gv)},yI=Promise.resolve(),Gv=(()=>{class e extends $r{_changeDetectorRef;callSetDisabledState;control=new PI;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new oe;constructor(t,i,r,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=XP(this,o)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),KP(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){Ku(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){yI.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&we(i);yI.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?zP(t,this._parent):[t]}static \u0275fac=function(i){return new(i||e)(ee(Hr,9),ee(mc,10),ee(Vv,10),ee(Ju,10),ee(ht,8),ee(nf,8))};static \u0275dir=B({type:e,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[st([nL]),Qe,xt]})}return e})();var iL=(()=>{class e extends Hr{callSetDisabledState;get submitted(){return je(this._submittedReactive)}set submitted(t){this._submittedReactive.set(t)}_submitted=Ge(()=>this._submittedReactive());_submittedReactive=Te(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(t,i,r){super(),this.callSetDisabledState=r,this._setValidators(t),this._setAsyncValidators(i)}ngOnChanges(t){this.onChanges(t)}ngOnDestroy(){this.onDestroy()}onChanges(t){this._checkFormPresent(),t.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Xu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(t){let i=this.form.get(t.path);return Ku(i,t,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(t),i}getControl(t){return this.form.get(t.path)}removeControl(t){mI(t.control||null,t,!1),JP(this.directives,t)}addFormGroup(t){this._setUpFormContainer(t)}removeFormGroup(t){this._cleanUpFormContainer(t)}getFormGroup(t){return this.form.get(t.path)}getFormArray(t){return this.form.get(t.path)}addFormArray(t){this._setUpFormContainer(t)}removeFormArray(t){this._cleanUpFormContainer(t)}updateModel(t,i){this.form.get(t.path).setValue(i)}onReset(){this.resetForm()}resetForm(t=void 0,i={}){this.form.reset(t,i),this._submittedReactive.set(!1)}onSubmit(t){return this.submitted=!0,FI(this.form,this.directives),this.ngSubmit.emit(t),this.form._events.next(new Wu(this.control)),t?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(t=>{let i=t.control,r=this.form.get(t.path);i!==r&&(mI(i||null,t),tL(r)&&(Ku(r,t,this.callSetDisabledState),t.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(t){let i=this.form.get(t.path);OI(i,t),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(t){let i=this.form?.get(t.path);i&&ZP(i,t)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){$v(this.form,this),this._oldForm&&Xu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||e)(ee(mc,10),ee(Vv,10),ee(nf,8))};static \u0275dir=B({type:e,features:[Qe,xt]})}return e})();var rL={provide:Hr,useExisting:Vt(()=>Wv)},Wv=(()=>{class e extends iL{form=null;ngSubmit=new oe;get control(){return this.form}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275dir=B({type:e,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&pe("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[st([rL]),Qe]})}return e})();var oL=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({})}return e})();var rf=(()=>{class e{static withConfig(t){return{ngModule:e,providers:[{provide:nf,useValue:t.callSetDisabledState??Hv}]}}static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[oL]})}return e})();var aL=["*"];var cL=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],lL=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],dL=new y("MAT_CARD_CONFIG"),of=(()=>{class e{appearance;constructor(){let t=f(dL,{optional:!0});this.appearance=t?.appearance||"raised"}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&te("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:aL,decls:1,vars:0,template:function(i,r){i&1&&(Be(),se(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return e})(),sf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return e})();var af=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return e})(),cf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return e})(),LI=(()=>{class e{align="start";static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&te("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return e})(),lf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:lL,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(Be(cL),se(0),rt(1,"div",0),se(2,1),ot(),se(3,2))},encapsulation:2,changeDetection:0})}return e})();var df=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","mat-card-avatar",""],["","matCardAvatar",""]],hostAttrs:[1,"mat-mdc-card-avatar"]})}return e})();var uf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[Ue]})}return e})();var ff=(()=>{class e{isErrorState(t,i){return!!(t&&t.invalid&&(t.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var hf=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,t,i,r,o){this._defaultMatcher=n,this.ngControl=t,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,t=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,t)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var qv=class{_box;_destroyed=new M;_resizeSubject=new M;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(t=>this._resizeSubject.next(t)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new H(t=>{let i=this._resizeSubject.subscribe(t);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(_e(t=>t.some(i=>i.target===n)),th({bufferSize:1,refCount:!0}),Ne(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},VI=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=f(j);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,t]of this._observers)t.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(t,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new qv(r)),this._observers.get(r).observe(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var uL=["notch"],fL=["matFormFieldNotchedOutline",""],hL=["*"],BI=["iconPrefixContainer"],UI=["textPrefixContainer"],HI=["iconSuffixContainer"],$I=["textSuffixContainer"],pL=["textField"],mL=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],gL=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function vL(e,n){e&1&&Ce(0,"span",21)}function yL(e,n){if(e&1&&(g(0,"label",20),se(1,1),k(2,vL,1,0,"span",21),v()),e&2){let t=q(2);ye("floating",t._shouldLabelFloat())("monitorResize",t._hasOutline())("id",t._labelId),be("for",t._control.disableAutomaticLabeling?null:t._control.id),w(2),N(!t.hideRequiredMarker&&t._control.required?2:-1)}}function _L(e,n){if(e&1&&k(0,yL,3,5,"label",20),e&2){let t=q();N(t._hasFloatingLabel()?0:-1)}}function bL(e,n){e&1&&Ce(0,"div",7)}function wL(e,n){}function EL(e,n){if(e&1&&Pi(0,wL,0,0,"ng-template",13),e&2){q(2);let t=Co(1);ye("ngTemplateOutlet",t)}}function CL(e,n){if(e&1&&(g(0,"div",9),k(1,EL,1,1,null,13),v()),e&2){let t=q();ye("matFormFieldNotchedOutlineOpen",t._shouldLabelFloat()),w(),N(t._forceDisplayInfixLabel()?-1:1)}}function DL(e,n){e&1&&(g(0,"div",10,2),se(2,2),v())}function IL(e,n){e&1&&(g(0,"div",11,3),se(2,3),v())}function xL(e,n){}function SL(e,n){if(e&1&&Pi(0,xL,0,0,"ng-template",13),e&2){q();let t=Co(1);ye("ngTemplateOutlet",t)}}function ML(e,n){e&1&&(g(0,"div",14,4),se(2,4),v())}function TL(e,n){e&1&&(g(0,"div",15,5),se(2,5),v())}function AL(e,n){e&1&&Ce(0,"div",16)}function RL(e,n){e&1&&(g(0,"div",18),se(1,6),v())}function kL(e,n){if(e&1&&(g(0,"mat-hint",22),C(1),v()),e&2){let t=q(2);ye("id",t._hintLabelId),w(),Re(t.hintLabel)}}function NL(e,n){if(e&1&&(g(0,"div",19),k(1,kL,2,2,"mat-hint",22),se(2,7),Ce(3,"div",23),se(4,8),v()),e&2){let t=q();w(),N(t.hintLabel?1:-1)}}var gc=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["mat-label"]]})}return e})(),OL=new y("MatError");var Yv=(()=>{class e{align="start";id=f(bt).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Ln("id",r.id),be("align",null),te("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),FL=new y("MatPrefix");var KI=new y("MatSuffix"),Zv=(()=>{class e{set _isTextSelector(t){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[st([{provide:KI,useExisting:e}])]})}return e})(),QI=new y("FloatingLabelParent"),zI=(()=>{class e{_elementRef=f(Q);get floating(){return this._floating}set floating(t){this._floating=t,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(t){this._monitorResize=t,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=f(VI);_ngZone=f(j);_parent=f(QI);_resizeSubscription=new Ie;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return PL(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&te("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function PL(e){let n=e;if(n.offsetParent!==null)return n.scrollWidth;let t=n.cloneNode(!0);t.style.setProperty("position","absolute"),t.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(t);let i=t.scrollWidth;return t.remove(),i}var GI="mdc-line-ripple--active",pf="mdc-line-ripple--deactivating",WI=(()=>{class e{_elementRef=f(Q);_cleanupTransitionEnd;constructor(){let t=f(j),i=f(it);t.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let t=this._elementRef.nativeElement.classList;t.remove(pf),t.add(GI)}deactivate(){this._elementRef.nativeElement.classList.add(pf)}_handleTransitionEnd=t=>{let i=this._elementRef.nativeElement.classList,r=i.contains(pf);t.propertyName==="opacity"&&r&&i.remove(GI,pf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),qI=(()=>{class e{_elementRef=f(Q);_ngZone=f(j);open=!1;_notch;ngAfterViewInit(){let t=this._elementRef.nativeElement,i=t.querySelector(".mdc-floating-label");i?(t.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):t.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(t){let i=this._notch.nativeElement;!this.open||!t?i.style.width="":i.style.width=`calc(${t}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(t){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${t}px)`)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&bn(uL,5),i&2){let o;ce(o=le())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&te("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:fL,ngContentSelectors:hL,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Be(),At(0,"div",1),rt(1,"div",2,0),se(3),ot(),At(4,"div",3))},encapsulation:2,changeDetection:0})}return e})(),Kv=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e})}return e})();var Qv=new y("MatFormField"),LL=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),YI="fill",jL="auto",ZI="fixed",VL="translateY(-50%)",mf=(()=>{class e{_elementRef=f(Q);_changeDetectorRef=f(ht);_platform=f(We);_idGenerator=f(bt);_ngZone=f(j);_defaults=f(LL,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=da("iconPrefixContainer");_textPrefixContainerSignal=da("textPrefixContainer");_iconSuffixContainerSignal=da("iconSuffixContainer");_textSuffixContainerSignal=da("textSuffixContainer");_prefixSuffixContainers=Ge(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(t=>t?.nativeElement).filter(t=>t!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Nw(gc);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(t){this._hideRequiredMarker=is(t)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||jL}set floatLabel(t){t!==this._floatLabel&&(this._floatLabel=t,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(t){let i=t||this._defaults?.appearance||YI;this._appearanceSignal.set(i)}_appearanceSignal=Te(YI);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||ZI}set subscriptSizing(t){this._subscriptSizing=t||this._defaults?.subscriptSizing||ZI}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(t){this._hintLabel=t,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(t){this._explicitFormFieldControl=t}_destroyed=new M;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Mt();constructor(){let t=this._defaults,i=f(hi);t&&(t.appearance&&(this.appearance=t.appearance),this._hideRequiredMarker=!!t?.hideRequiredMarker,t.color&&(this.color=t.color)),hn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Ge(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(t){let i=this._control,r="mat-mdc-form-field-type-";t&&this._elementRef.nativeElement.classList.remove(r+t.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(cn([void 0,void 0]),T(()=>[i.errorState,i.userAriaDescribedBy]),Xf(),_e(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ne(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(t=>!t._isText),this._hasTextPrefix=!!this._prefixChildren.find(t=>t._isText),this._hasIconSuffix=!!this._suffixChildren.find(t=>!t._isText),this._hasTextSuffix=!!this._suffixChildren.find(t=>t._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),jt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let t=this._control.focused;t&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!t&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",t),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",t)}_syncOutlineLabelOffset(){Lw({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let t of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(t,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:t=>this._writeOutlinedLabelStyles(t())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Ge(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(t){let i=this._control?this._control.ngControl:null;return i&&i[t]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let t=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&t.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?t.push(o.id):this._hintLabel&&t.push(this._hintLabelId),s&&t.push(s.id)}else this._errorChildren&&t.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||t;r=t.concat(i.filter(s=>s&&!o.includes(s)))}else r=t;this._control.setDescribedByIds(r),this._describedByIds=t}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let t=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=t?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,p=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,m=`var(--mat-mdc-form-field-label-transform, ${VL} translateX(${p}))`,b=s+a+c+l;return[m,b]}_writeOutlinedLabelStyles(t){if(t!==null){let[i,r]=t;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let t=this._elementRef.nativeElement;if(t.getRootNode){let i=t.getRootNode();return i&&i!==t}return document.documentElement.contains(t)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(gd(o,r._labelChild,gc,5),si(o,Kv,5)(o,FL,5)(o,KI,5)(o,OL,5)(o,Yv,5)),i&2){yd();let s;ce(s=le())&&(r._formFieldControl=s.first),ce(s=le())&&(r._prefixChildren=s),ce(s=le())&&(r._suffixChildren=s),ce(s=le())&&(r._errorChildren=s),ce(s=le())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(vd(r._iconPrefixContainerSignal,BI,5)(r._textPrefixContainerSignal,UI,5)(r._iconSuffixContainerSignal,HI,5)(r._textSuffixContainerSignal,$I,5),bn(pL,5)(BI,5)(UI,5)(HI,5)($I,5)(zI,5)(qI,5)(WI,5)),i&2){yd(4);let o;ce(o=le())&&(r._textField=o.first),ce(o=le())&&(r._iconPrefixContainer=o.first),ce(o=le())&&(r._textPrefixContainer=o.first),ce(o=le())&&(r._iconSuffixContainer=o.first),ce(o=le())&&(r._textSuffixContainer=o.first),ce(o=le())&&(r._floatingLabel=o.first),ce(o=le())&&(r._notchedOutline=o.first),ce(o=le())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&te("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[st([{provide:Qv,useExisting:e},{provide:QI,useExisting:e}])],ngContentSelectors:gL,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Be(mL),Pi(0,_L,1,1,"ng-template",null,0,Lm),g(2,"div",6,1),pe("click",function(s){return r._control.onContainerClick(s)}),k(4,bL,1,0,"div",7),g(5,"div",8),k(6,CL,2,2,"div",9),k(7,DL,3,0,"div",10),k(8,IL,3,0,"div",11),g(9,"div",12),k(10,SL,1,1,null,13),se(11),v(),k(12,ML,3,0,"div",14),k(13,TL,3,0,"div",15),v(),k(14,AL,1,0,"div",16),v(),g(15,"div",17),k(16,RL,2,0,"div",18)(17,NL,5,1,"div",19),v()),i&2){let o;w(2),te("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),w(2),N(!r._hasOutline()&&!r._control.disabled?4:-1),w(2),N(r._hasOutline()?6:-1),w(),N(r._hasIconPrefix?7:-1),w(),N(r._hasTextPrefix?8:-1),w(2),N(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),w(2),N(r._hasTextSuffix?12:-1),w(),N(r._hasIconSuffix?13:-1),w(),N(r._hasOutline()?-1:14),w(),te("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();w(),N((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[zI,qI,Jm,WI,Yv],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return e})();var BL=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],UL=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function HL(e,n){e&1&&(g(0,"span",3),se(1,1),v())}function $L(e,n){e&1&&(g(0,"span",6),se(1,2),v())}var zL=["*"];var GL=new y("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),XI=new y("MatChipAvatar"),JI=new y("MatChipTrailingIcon"),ex=new y("MatChipEdit"),tx=new y("MatChipRemove"),ix=new y("MatChip"),rx=(()=>{class e{_elementRef=f(Q);_parentChip=f(ix);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(t){this._disabled=t}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){f(St).load(Ur),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(be("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),te("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",we],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?-1:Vn(t)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return e})(),WL=(()=>{class e extends rx{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(t){!this.disabled&&this._isPrimary&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ft(e)))(r||e)}})();static \u0275dir=B({type:e,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&pe("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(be("tabindex",r._getTabindex()),te("mdc-evolution-chip__action--presentational",!1))},features:[Qe]})}return e})();var vc=(()=>{class e{_changeDetectorRef=f(ht);_elementRef=f(Q);_tagName=f(Rw);_ngZone=f(j);_focusMonitor=f(Lr);_globalRippleOptions=f(ac,{optional:!0});_document=f(W);_onFocus=new M;_onBlur=new M;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Mt();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=f(bt).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(t){this._value=t}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(t){this._disabled=t}_disabled=!1;removed=new oe;destroyed=new oe;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=f(Lu);_injector=f(re);constructor(){let t=f(St);t.load(Ur),t.load(vv),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=jt(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe(),this.destroyed.emit({chip:this}),this.destroyed.complete()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(t){(t.keyCode===8&&!t.repeat||t.keyCode===46)&&(t.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(t){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===t||r.contains(t)})}_getActions(){let t=[];return this.editIcon&&t.push(this.editIcon),this.primaryAction&&t.push(this.primaryAction),this.removeIcon&&t.push(this.removeIcon),t}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(t){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{let i=t!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&si(o,XI,5)(o,ex,5)(o,JI,5)(o,tx,5)(o,XI,5)(o,JI,5)(o,ex,5)(o,tx,5),i&2){let s;ce(s=le())&&(r.leadingIcon=s.first),ce(s=le())&&(r.editIcon=s.first),ce(s=le())&&(r.trailingIcon=s.first),ce(s=le())&&(r.removeIcon=s.first),ce(s=le())&&(r._allLeadingIcons=s),ce(s=le())&&(r._allTrailingIcons=s),ce(s=le())&&(r._allEditIcons=s),ce(s=le())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&bn(WL,5),i&2){let o;ce(o=le())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&pe("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Ln("id",r.id),be("role",r.role)("aria-label",r.ariaLabel),wn("mat-"+(r.color||"primary")),te("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",we],highlighted:[2,"highlighted","highlighted",we],disableRipple:[2,"disableRipple","disableRipple",we],disabled:[2,"disabled","disabled",we]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[st([{provide:ix,useExisting:e}])],ngContentSelectors:UL,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(Be(BL),Ce(0,"span",0),g(1,"span",1)(2,"span",2),k(3,HL,2,0,"span",3),g(4,"span",4),se(5),Ce(6,"span",5),v()()(),k(7,$L,2,0,"span",6)),i&2&&(w(3),N(r.leadingIcon?3:-1),w(4),N(r._hasTrailingIcon()?7:-1))},dependencies:[rx],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var gf=(()=>{class e{_elementRef=f(Q);_changeDetectorRef=f(ht);_dir=f(hi,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new M;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(t=>t._onFocus)}get chipDestroyedChanges(){return this._getChipStream(t=>t.destroyed)}get chipRemovedChanges(){return this._getChipStream(t=>t.removed)}get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(t){this._explicitRole=t}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new ni;constructor(){}ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(t=>t._hasFocus())}_syncChipsState(){this._chips?.forEach(t=>{t._chipListDisabled=this._disabled,t._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(t){this._originatesFromChip(t)&&this._keyManager.onKeydown(t)}_isValidIndex(t){return t>=0&&t<this._chips.length}_allowFocusEscape(){let t=this._elementRef.nativeElement.tabIndex;t!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=t))}_getChipStream(t){return this._chips.changes.pipe(cn(null),He(()=>jt(...this._chips.map(t))))}_originatesFromChip(t){let i=t.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(cn(this._chips)).subscribe(t=>{let i=[];t.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new ic(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(t=>this._skipPredicate(t)),this.chipFocusChanges.pipe(Ne(this._destroyed)).subscribe(({chip:t})=>{let i=t._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(Ne(this._destroyed)).subscribe(t=>this._keyManager.withHorizontalOrientation(t))}_skipPredicate(t){return t.disabled}_trackChipSetChanges(){this._chips.changes.pipe(cn(null),Ne(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(Ne(this._destroyed)).subscribe(t=>{let r=this._chips.toArray().indexOf(t.chip),o=t.chip._hasFocus(),s=t.chip._hadFocusOnRemove&&this._keyManager.activeItem&&t.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let t=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[t];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&si(o,vc,5),i&2){let s;ce(s=le())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&pe("keydown",function(s){return r._handleKeydown(s)}),i&2&&be("role",r.role)},inputs:{disabled:[2,"disabled","disabled",we],role:"role",tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:Vn(t)]},ngContentSelectors:zL,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(Be(),rt(0,"div",0),se(1),ot())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var vf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({providers:[ff,{provide:GL,useValue:{separatorKeyCodes:[13]}}],imports:[ju,Ue]})}return e})();var zr=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[jD,mf,Ue]})}return e})();function sx(e){return Error(`Unable to find icon with the name "${e}"`)}function YL(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function ax(e){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${e}".`)}function cx(e){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${e}".`)}var pi=class{url;svgText;options;svgElement=null;constructor(n,t,i){this.url=n,this.svgText=t,this.options=i}},dx=(()=>{class e{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(t,i,r,o){this._httpClient=t,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(t,i,r){return this.addSvgIconInNamespace("",t,i,r)}addSvgIconLiteral(t,i,r){return this.addSvgIconLiteralInNamespace("",t,i,r)}addSvgIconInNamespace(t,i,r,o){return this._addSvgIconConfig(t,i,new pi(r,null,o))}addSvgIconResolver(t){return this._resolvers.push(t),this}addSvgIconLiteralInNamespace(t,i,r,o){let s=this._sanitizer.sanitize(nt.HTML,r);if(!s)throw cx(r);let a=jr(s);return this._addSvgIconConfig(t,i,new pi("",a,o))}addSvgIconSet(t,i){return this.addSvgIconSetInNamespace("",t,i)}addSvgIconSetLiteral(t,i){return this.addSvgIconSetLiteralInNamespace("",t,i)}addSvgIconSetInNamespace(t,i,r){return this._addSvgIconSetConfig(t,new pi(i,null,r))}addSvgIconSetLiteralInNamespace(t,i,r){let o=this._sanitizer.sanitize(nt.HTML,i);if(!o)throw cx(i);let s=jr(o);return this._addSvgIconSetConfig(t,new pi("",s,r))}registerFontClassAlias(t,i=t){return this._fontCssClassesByAlias.set(t,i),this}classNameForFontAlias(t){return this._fontCssClassesByAlias.get(t)||t}setDefaultFontSetClass(...t){return this._defaultFontSetClass=t,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(t){let i=this._sanitizer.sanitize(nt.RESOURCE_URL,t);if(!i)throw ax(t);let r=this._cachedIconsByUrl.get(i);return r?R(yf(r)):this._loadSvgIconFromConfig(new pi(t,null)).pipe(Je(o=>this._cachedIconsByUrl.set(i,o)),T(o=>yf(o)))}getNamedSvgIcon(t,i=""){let r=lx(i,t),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,t),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(t,s):er(sx(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(t){return t.svgText?R(yf(this._svgElementFromConfig(t))):this._loadSvgIconFromConfig(t).pipe(T(i=>yf(i)))}_getSvgFromIconSetConfigs(t,i){let r=this._extractIconWithNameFromAnySet(t,i);if(r)return R(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Et(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(nt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),R(null)})));return vs(o).pipe(T(()=>{let s=this._extractIconWithNameFromAnySet(t,i);if(!s)throw sx(t);return s}))}_extractIconWithNameFromAnySet(t,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(t)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,t,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(t){return this._fetchIcon(t).pipe(Je(i=>t.svgText=i),T(()=>this._svgElementFromConfig(t)))}_loadSvgIconSetFromConfig(t){return t.svgText?R(null):this._fetchIcon(t).pipe(Je(i=>t.svgText=i))}_extractSvgIconFromSet(t,i,r){let o=t.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(jr("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(t){let i=this._document.createElement("DIV");i.innerHTML=t;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(t){let i=this._svgElementFromString(jr("<svg></svg>")),r=t.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<t.childNodes.length;o++)t.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(t.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(t,i){return t.setAttribute("fit",""),t.setAttribute("height","100%"),t.setAttribute("width","100%"),t.setAttribute("preserveAspectRatio","xMidYMid meet"),t.setAttribute("focusable","false"),i&&i.viewBox&&t.setAttribute("viewBox",i.viewBox),t}_fetchIcon(t){let{url:i,options:r}=t,o=r?.withCredentials??!1;if(!this._httpClient)throw YL();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(nt.RESOURCE_URL,i);if(!s)throw ax(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(T(l=>jr(l)),ir(()=>this._inProgressUrlFetches.delete(s)),rr());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(t,i,r){return this._svgIconConfigs.set(lx(t,i),r),this}_addSvgIconSetConfig(t,i){let r=this._iconSetConfigs.get(t);return r?r.push(i):this._iconSetConfigs.set(t,[i]),this}_svgElementFromConfig(t){if(!t.svgElement){let i=this._svgElementFromString(t.svgText);this._setSvgAttributes(i,t.options),t.svgElement=i}return t.svgElement}_getIconConfigFromResolvers(t,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,t);if(o)return ZL(o)?new pi(o.url,null,o.options):new pi(o,null)}}static \u0275fac=function(i){return new(i||e)(I(To,8),I(Ca),I(W,8),I(Ye))};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function yf(e){return e.cloneNode(!0)}function lx(e,n){return e+":"+n}function ZL(e){return!!(e.url&&e.options)}var KL=["*"],QL=new y("MAT_ICON_DEFAULT_OPTIONS"),XL=new y("mat-icon-location",{providedIn:"root",factory:()=>{let e=f(W),n=e?e.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),ux=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],JL=ux.map(e=>`[${e}]`).join(", "),ej=/^url\(['"]?#(.*?)['"]?\)$/,_f=(()=>{class e{_elementRef=f(Q);_iconRegistry=f(dx);_location=f(XL);_errorHandler=f(Ye);_defaultColor;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(t){t!==this._svgIcon&&(t?this._updateSvgIcon(t):this._svgIcon&&this._clearSvgElement(),this._svgIcon=t)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(t){let i=this._cleanupFontValue(t);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(t){let i=this._cleanupFontValue(t);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=Ie.EMPTY;constructor(){let t=f(new xr("aria-hidden"),{optional:!0}),i=f(QL,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),t||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(t){if(!t)return["",""];let i=t.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${t}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let t=this._elementsWithExternalReferences;if(t&&t.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(t){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(t),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(t)}_clearSvgElement(){let t=this._elementRef.nativeElement,i=t.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=t.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let t=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>t.classList.remove(r)),i.forEach(r=>t.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&t.classList.remove(this._previousFontIconClass),this.fontIcon&&t.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(t){return typeof t=="string"?t.trim().split(" ")[0]:t}_prependPathToReferences(t){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${t}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(t){let i=t.querySelectorAll(JL),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)ux.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(ej):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(t){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),t){let[i,r]=this._splitIconName(t);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(ct(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(be("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),wn(r.color?"mat-"+r.color:""),te("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",we],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:KL,decls:1,vars:0,template:function(i,r){i&1&&(Be(),se(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return e})(),bf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[Ue]})}return e})();var tj=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return e})(),nj={passive:!0},hx=(()=>{class e{_platform=f(We);_ngZone=f(j);_renderer=f(ut).createRenderer(null,null);_styleLoader=f(St);_monitoredElements=new Map;constructor(){}monitor(t){if(!this._platform.isBrowser)return xe;this._styleLoader.load(tj);let i=Dn(t),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new M,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,nj)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(t){let i=Dn(t),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((t,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=E({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var px=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({})}return e})();var mx=new y("MAT_INPUT_VALUE_ACCESSOR");var ij=["button","checkbox","file","hidden","image","radio","range","reset","submit"],rj=new y("MAT_INPUT_CONFIG"),gx=(()=>{class e{_elementRef=f(Q);_platform=f(We);ngControl=f($r,{optional:!0,self:!0});_autofillMonitor=f(hx);_ngZone=f(j);_formField=f(Qv,{optional:!0});_renderer=f(it);_uid=f(bt).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=f(rj,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new M;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=is(t),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(t){this._id=t||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Hu.required)??!1}set required(t){this._required=is(t)}_required;get type(){return this._type}set type(t){this._type=t||"text",this._validateType(),!this._isTextarea&&Sv().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(t){this._errorStateTracker.matcher=t}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(t){t!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(t):this._inputValueAccessor.value=t,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(t){this._readonly=is(t)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(t){this._errorStateTracker.errorState=t}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(t=>Sv().has(t));constructor(){let t=f(zv,{optional:!0}),i=f(Wv,{optional:!0}),r=f(ff),o=f(mx,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?Li(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new hf(r,this.ngControl,i,t,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&hn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(t=>{this.autofilled=t.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(t){this._elementRef.nativeElement.focus(t)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(t){if(t!==this.focused){if(!this._isNativeSelect&&t&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=t,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let t=this._elementRef.nativeElement.value;this._previousNativeValue!==t&&(this._previousNativeValue=t,this.stateChanges.next())}_dirtyCheckPlaceholder(){let t=this._getPlaceholder();if(t!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=t,t?i.setAttribute("placeholder",t):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){ij.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let t=this._elementRef.nativeElement.validity;return t&&t.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let t=this._elementRef.nativeElement,i=t.options[0];return this.focused||t.multiple||!this.empty||!!(t.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(t){let i=this._elementRef.nativeElement;t.length?i.setAttribute("aria-describedby",t.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let t=this._elementRef.nativeElement;return this._isNativeSelect&&(t.multiple||t.size>1)}_iOSKeyupListener=t=>{let i=t.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||e)};static \u0275dir=B({type:e,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&pe("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Ln("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),be("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),te("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",we]},exportAs:["matInput"],features:[st([{provide:Kv,useExisting:e}]),xt]})}return e})(),wf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[zr,zr,px,Ue]})}return e})();var Xv=WC("launch"),Ef=qa(Xv,e=>e.launches),Pie=qa(Xv,e=>e.loading),Cf=qa(Xv,e=>e.favoriteIds);var sj=["mat-internal-form-field",""],aj=["*"],vx=(()=>{class e{labelPosition="after";static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&te("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:sj,ngContentSelectors:aj,decls:1,vars:0,template:function(i,r){i&1&&(Be(),se(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var cj=["switch"],lj=["*"];function dj(e,n){e&1&&(g(0,"span",11),Ps(),g(1,"svg",13),Ce(2,"path",14),v(),g(3,"svg",15),Ce(4,"path",16),v()())}var uj=new y("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),Df=class{source;checked;constructor(n,t){this.source=n,this.checked=t}},Jv=(()=>{class e{_elementRef=f(Q);_focusMonitor=f(Lr);_changeDetectorRef=f(ht);defaults=f(uj);_onChange=t=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(t){return new Df(this,t)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Mt();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(t){this._checked=t,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new oe;toggleChange=new oe;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){f(St).load(Ur);let t=f(new xr("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=t==null?0:parseInt(t)||0,this.color=i.color||"accent",this.id=this._uniqueId=f(bt).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{t==="keyboard"||t==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):t||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(t){t.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(t){this.checked=!!t}registerOnChange(t){this._onChange=t}registerOnTouched(t){this._onTouched=t}validate(t){return this.required&&t.value!==!0?{required:!0}:null}registerOnValidatorChange(t){this._validatorOnChange=t}setDisabledState(t){this.disabled=t,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new Df(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&bn(cj,5),i&2){let o;ce(o=le())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(Ln("id",r.id),be("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),wn(r.color?"mat-"+r.color:""),te("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",we],color:"color",disabled:[2,"disabled","disabled",we],disableRipple:[2,"disableRipple","disableRipple",we],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:Vn(t)],checked:[2,"checked","checked",we],hideIcon:[2,"hideIcon","hideIcon",we],disabledInteractive:[2,"disabledInteractive","disabledInteractive",we]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[st([{provide:Ju,useExisting:Vt(()=>e),multi:!0},{provide:mc,useExisting:e,multi:!0}]),xt],ngContentSelectors:lj,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(Be(),g(0,"div",1)(1,"button",2,0),pe("click",function(){return r._handleClick()}),Ce(3,"div",3)(4,"span",4),g(5,"span",5)(6,"span",6)(7,"span",7),Ce(8,"span",8),v(),g(9,"span",9),Ce(10,"span",10),v(),k(11,dj,5,0,"span",11),v()()(),g(12,"label",12),pe("click",function(s){return s.stopPropagation()}),se(13),v()()),i&2){let o=Co(2);ye("labelPosition",r.labelPosition),w(),te("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),ye("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),be("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),w(9),ye("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),w(),N(r.hideIcon?-1:11),w(),ye("for",r.buttonId),be("id",r._labelId)}},dependencies:[rI,vx],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return e})(),yx=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[Jv,Ue]})}return e})();var hj=e=>({hide:e});function pj(e,n){if(e&1){let t=_n();g(0,"a",18),pe("click",function(r){zt(t);let o=q().$implicit,s=q();return Gt(s.addToFavorite(r,o))}),g(1,"mat-icon",19),C(2,"star_border"),v()()}}function mj(e,n){if(e&1){let t=_n();g(0,"a",20),pe("click",function(r){zt(t);let o=q().$implicit,s=q();return Gt(s.removeFromFavorite(r,o))}),g(1,"mat-icon",21),C(2,"star"),v()()}}function gj(e,n){if(e&1){let t=_n();g(0,"mat-card",11),pe("click",function(){let r=zt(t).$implicit,o=q();return Gt(o.goToLaunch(r))}),g(1,"mat-card-header"),Ce(2,"img",12),g(3,"mat-card-title"),C(4),v(),g(5,"mat-card-subtitle"),C(6),v(),k(7,pj,3,0,"a",13),k(8,mj,3,0,"a",14),v(),g(9,"mat-card-content",15)(10,"p",16)(11,"mat-icon"),C(12,"calendar_today"),v(),C(13),jn(14,"date"),v()(),g(15,"mat-card-actions",17)(16,"mat-chip-set")(17,"mat-chip"),C(18),v()()()()}if(e&2){let t=n.$implicit,i=q();ye("ngClass",Pm(15,hj,i.isCardHidden(t))),w(2),ye("src",t.links.patch==null?null:t.links.patch.small,vt),w(2),Re(t.name),w(2),Fe("Flight n\xB0",t.flight_number),w(),N(i.isFavouriteLaunch(t)?-1:7),w(),N(i.isFavouriteLaunch(t)?8:-1),w(5),Fe(" ",Ir(14,12,t.date_local,"dd/MM/yyyy")," "),w(4),te("status-success",t.success)("status-error",!t.success),w(),Fe(" ",t.success?"Success":"Failure"," ")}}var If=class e{constructor(n,t){this.store=n;this.router=t;this.filteredLaunches=Ge(()=>this.store.selectSignal(Ef)().filter(r=>r.name.toUpperCase().trim().includes(this.searchTerm().toUpperCase().trim())))}filteredLaunches;searchTerm=$m("");favoritesOnly=$m(!1);ngOnInit(){this.loadLaunches()}loadLaunches(){this.store.dispatch(Go()),this.store.dispatch(qo())}goToLaunch(n){let t=n.id;this.router.navigate(["launch",t])}addToFavorite(n,t){n.stopPropagation(),this.store.dispatch(Yo({launchId:t.id}))}removeFromFavorite(n,t){n.stopPropagation(),this.store.dispatch(Zo({launchId:t.id}))}isFavouriteLaunch(n){return this.store.selectSignal(Cf)().includes(n.id)}refreshData(){this.loadLaunches()}isCardHidden(n){return this.favoritesOnly()&&!this.isFavouriteLaunch(n)}static \u0275fac=function(t){return new(t||e)(ee(Lt),ee($i))};static \u0275cmp=K({type:e,selectors:[["app-launches-list"]],inputs:{searchTerm:[1,"searchTerm"],favoritesOnly:[1,"favoritesOnly"]},outputs:{searchTerm:"searchTermChange",favoritesOnly:"favoritesOnlyChange"},decls:20,vars:2,consts:[[1,"container-launches"],[1,"mat-headline-medium","title"],[1,"btns-container"],["labelPosition","before",3,"ngModelChange","ngModel"],["title","Refresh data","matButton","outlined",1,"circle-btn",3,"click"],["appearance","outline",1,"search-field"],["matInput","","type","text","placeholder","Ex. Crew-1",3,"ngModelChange","ngModel"],["matSuffix",""],[1,"launches-grid-wrapper"],[1,"launches-grid"],[1,"launch-card",3,"ngClass"],[1,"launch-card",3,"click","ngClass"],["mat-card-avatar","","alt","Mission's patch",3,"src"],["title","Add to favorites","matButton","outlined",1,"circle-btn","mark-favorite-btn"],["title","Remove from favorites","matButton","outlined",1,"circle-btn","mark-favorite-btn"],[1,"card-content"],[1,"date-text"],[1,"card-actions"],["title","Add to favorites","matButton","outlined",1,"circle-btn","mark-favorite-btn",3,"click"],["title","Add to favorites"],["title","Remove from favorites","matButton","outlined",1,"circle-btn","mark-favorite-btn",3,"click"],["title","Remove from favorites"]],template:function(t,i){t&1&&(g(0,"div",0)(1,"h1",1)(2,"span"),C(3,"SpaceX Launch Explorer"),v(),g(4,"div",2)(5,"mat-slide-toggle",3),sa("ngModelChange",function(o){return _d(i.favoritesOnly,o)||(i.favoritesOnly=o),o}),C(6,"Show only favorites"),v(),g(7,"a",4),pe("click",function(){return i.refreshData()}),g(8,"mat-icon"),C(9,"refresh"),v()()()(),g(10,"mat-form-field",5)(11,"mat-label"),C(12,"Search for a mission..."),v(),g(13,"input",6),sa("ngModelChange",function(o){return _d(i.searchTerm,o)||(i.searchTerm=o),o}),v(),g(14,"mat-icon",7),C(15,"search"),v()(),g(16,"mat-card",8)(17,"div",9),Xt(18,gj,19,17,"mat-card",10,Qt),v()()()),t&2&&(w(5),oa("ngModel",i.favoritesOnly),w(8),oa("ngModel",i.searchTerm),w(5),Jt(i.filteredLaunches()))},dependencies:[Io,Xm,rf,ef,AI,Gv,uf,of,LI,df,af,lf,cf,sf,zr,mf,gc,Zv,wf,gx,bf,_f,vf,vc,gf,cc,rs,yx,Jv,ga],styles:[".container-launches[_ngcontent-%COMP%]{padding:24px;max-width:1200px;margin:0 auto;height:calc(100% - 48px);display:flex;flex-direction:column}.container-launches[_ngcontent-%COMP%]   .launches-grid-wrapper[_ngcontent-%COMP%]{flex-grow:1;height:0px;overflow:hidden;border-radius:5px;background-color:#fff}.title[_ngcontent-%COMP%]{margin-bottom:24px;color:#1e293b}.search-field[_ngcontent-%COMP%]{width:100%;margin-bottom:24px}.launches-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:24px;height:calc(100% - 48px);overflow:auto;padding:24px;align-items:flex-start}.launch-card[_ngcontent-%COMP%]{display:flex;flex-direction:column;justify-content:space-between;transition:transform .2s,box-shadow .2s;cursor:pointer}.launch-card[_ngcontent-%COMP%]:hover{transform:translateY(-4px);box-shadow:0 8px 16px #0000001a}.card-content[_ngcontent-%COMP%]{padding-top:16px}.date-text[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;color:#64748b;font-size:14px}.date-text[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:18px;width:18px;height:18px}.card-actions[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;padding:8px 16px}.status-success[class][_ngcontent-%COMP%]{background-color:#e2fbe8;color:#1e7e34}.status-error[class][_ngcontent-%COMP%]{background-color:#fde8e8;color:#c82333}[_nghost-%COMP%]     mat-card-header .mat-mdc-card-header-text{flex-grow:1}mat-slide-toggle[_ngcontent-%COMP%]{margin-right:5px}.hide[_ngcontent-%COMP%]{display:none!important}"]})};var xf=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,t){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=t.map(i=>this._trackTile(i))}_trackTile(n){let t=this._findMatchingGap(n.colspan);return this._markTilePosition(t,n),this.columnIndex=t+n.colspan,new ey(this.rowIndex,t)}_findMatchingGap(n){n>this.tracker.length;let t=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),t=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(t);continue}if(t=this.tracker.indexOf(0,this.columnIndex),t==-1){this._nextRow(),t=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(t);continue}i=this._findGapEndIndex(t),this.columnIndex=t+1}while(i-t<n||i==0);return Math.max(t,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let t=n+1;t<this.tracker.length;t++)if(this.tracker[t]!=0)return t;return this.tracker.length}_markTilePosition(n,t){for(let i=0;i<t.colspan;i++)this.tracker[n+i]=t.rowspan}},ey=class{row;col;constructor(n,t){this.row=n,this.col=t}};var ty=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[Ue]})}return e})();var _x=["*"];var vj=`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`,bx=new y("MAT_GRID_LIST"),oy=(()=>{class e{_element=f(Q);_gridList=f(bx,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(t){this._rowspan=Math.round(Qo(t))}get colspan(){return this._colspan}set colspan(t){this._colspan=Math.round(Qo(t))}_setStyle(t,i){this._element.nativeElement.style[t]=i}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&be("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:_x,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(Be(),rt(0,"div",0),se(1),ot())},styles:[`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var yj=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,yc=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,t,i,r){this._gutterSize=wx(n),this._rows=t.rowCount,this._rowspan=t.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,t){return`(${n}% - (${this._gutterSize} * ${t}))`}getTilePosition(n,t){return t===0?"0":Gr(`(${n} + ${this._gutterSize}) * ${t}`)}getTileSize(n,t){return`(${n} * ${t}) + (${t-1} * ${this._gutterSize})`}setStyle(n,t,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,t,r,o)}setColStyles(n,t,i,r){let o=this.getBaseTileSize(i,r),s=this._direction==="rtl"?"right":"left";n._setStyle(s,this.getTilePosition(o,t)),n._setStyle("width",Gr(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},ny=class extends yc{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,t,i,r){super.init(n,t,i,r),this.fixedRowHeight=wx(this.fixedRowHeight),yj.test(this.fixedRowHeight)}setRowStyles(n,t){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,t)),n._setStyle("height",Gr(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",Gr(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(t=>{t._setStyle("top",null),t._setStyle("height",null)})}},iy=class extends yc{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,t,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,t)),n._setStyle("paddingTop",Gr(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",Gr(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(t=>{t._setStyle("marginTop",null),t._setStyle("paddingTop",null)})}_parseRatio(n){let t=n.split(":");t.length,this.rowHeightRatio=parseFloat(t[0])/parseFloat(t[1])}},ry=class extends yc{setRowStyles(n,t){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,t)),n._setStyle("height",Gr(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(t=>{t._setStyle("top",null),t._setStyle("height",null)})}};function Gr(e){return`calc(${e})`}function wx(e){return e.match(/([A-Za-z%]+)$/)?e:`${e}px`}var _j="fit",Ex=(()=>{class e{_element=f(Q);_dir=f(hi,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(t){this._cols=Math.max(1,Math.round(Qo(t)))}get gutterSize(){return this._gutter}set gutterSize(t){this._gutter=`${t??""}`}get rowHeight(){return this._rowHeight}set rowHeight(t){let i=`${t??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(t){this._tileStyler&&this._tileStyler.reset(this),t===_j?this._tileStyler=new ry:t&&t.indexOf(":")>-1?this._tileStyler=new iy(t):this._tileStyler=new ny(t)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new xf);let t=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,t,this.cols,r),i.forEach((o,s)=>{let a=t.positions[s];this._tileStyler.setStyle(o,a.row,a.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(t){t&&(this._element.nativeElement.style[t[0]]=t[1])}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&si(o,oy,5),i&2){let s;ce(s=le())&&(r._tiles=s)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&be("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[st([{provide:bx,useExisting:e}])],ngContentSelectors:_x,decls:2,vars:0,template:function(i,r){i&1&&(Be(),rt(0,"div"),se(1),ot())},styles:[vj],encapsulation:2,changeDetection:0})}return e})(),Cx=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[ty,Ue,ty]})}return e})();function wj(e,n){if(e&1){let t=_n();g(0,"a",7),pe("click",function(){zt(t);let r=q();return Gt(r.refreshData())}),g(1,"mat-icon"),C(2,"refresh"),v()()}}function Ej(e,n){if(e&1){let t=_n();g(0,"a",16),pe("click",function(){zt(t);let r=q(2);return Gt(r.addToFavorite())}),g(1,"mat-icon"),C(2,"star_border"),v()()}}function Cj(e,n){if(e&1){let t=_n();g(0,"a",17),pe("click",function(){zt(t);let r=q(2);return Gt(r.removeFromFavorite())}),g(1,"mat-icon"),C(2,"star"),v()()}}function Dj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Date of the static fire engine test"),v(),g(3,"span"),C(4),jn(5,"date"),v()()),e&2){let t,i=q(2);w(4),Fe(" ",Ir(5,1,(t=i.currentLaunchSignal())==null?null:t.static_fire_date_utc,"dd/MM/yyyy HH:MM")," ")}}function Ij(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Details about the mission"),v(),g(3,"span"),C(4),v()()),e&2){let t,i=q(2);w(4),Fe(" ",(t=i.currentLaunchSignal())==null?null:t.details," ")}}function xj(e,n){if(e&1&&(g(0,"tr")(1,"td"),C(2),v(),g(3,"td"),C(4),v(),g(5,"td"),C(6),v(),g(7,"td"),C(8),v(),g(9,"td"),C(10),v(),g(11,"td"),C(12),v(),g(13,"td"),C(14),v(),g(15,"td"),C(16),v(),g(17,"td"),C(18),v()()),e&2){let t=n.$implicit;w(2),Re(t.core),w(2),Re(t.flight),w(2),Re(t.reused?"Yes":"No"),w(2),Re(t.landing_attempt?"Yes":"No"),w(2),Re(t.landing_success?"Yes":"No"),w(2),Re(t.landing_type||"-"),w(2),Re(t.landpad||"-"),w(2),Re(t.gridfins?"Yes":"No"),w(2),Re(t.legs?"Yes":"No")}}function Sj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Boosters used in this mission"),v(),g(3,"div",18)(4,"table")(5,"thead")(6,"tr")(7,"th"),C(8,"Booster Core ID"),v(),g(9,"th"),C(10,"Booster Core Flight N\xB0"),v(),g(11,"th"),C(12,"Booster Core Reused ?"),v(),g(13,"th"),C(14,"Landing Attempt"),v(),g(15,"th"),C(16,"Landing Successful"),v(),g(17,"th"),C(18,"Landing Type"),v(),g(19,"th"),C(20,"Landing Pad"),v(),g(21,"th"),C(22,"Grid-Fins-Equipped ?"),v(),g(23,"th"),C(24,"Landing-Legs-Equipped ?"),v()()(),g(25,"tbody"),Xt(26,xj,19,9,"tr",null,Qt),v()()()()),e&2){let t,i=q(2);w(26),Jt((t=i.currentLaunchSignal())==null?null:t.cores)}}function Mj(e,n){if(e&1&&(g(0,"p"),C(1),v()),e&2){let t=n.$implicit;w(),Re(t)}}function Tj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Payload (satellite, spacecraft) IDs"),v(),Xt(3,Mj,2,1,"p",null,Qt),v()),e&2){let t,i=q(2);w(3),Jt((t=i.currentLaunchSignal())==null?null:t.payloads)}}function Aj(e,n){if(e&1&&(g(0,"p"),C(1),v()),e&2){let t=n.$implicit;w(),Re(t)}}function Rj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Crew member IDs"),v(),Xt(3,Aj,2,1,"p",null,Qt),v()),e&2){let t,i=q(2);w(3),Jt((t=i.currentLaunchSignal())==null?null:t.crew)}}function kj(e,n){if(e&1&&(g(0,"p"),C(1),v()),e&2){let t=n.$implicit;w(),Re(t)}}function Nj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Ship IDs used for recovery or support operations"),v(),Xt(3,kj,2,1,"p",null,Qt),v()),e&2){let t,i=q(2);w(3),Jt((t=i.currentLaunchSignal())==null?null:t.ships)}}function Oj(e,n){if(e&1&&(g(0,"p"),C(1),v()),e&2){let t=n.$implicit;w(),Re(t)}}function Fj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Dragon capsule IDs used on the mission"),v(),Xt(3,Oj,2,1,"p",null,Qt),v()),e&2){let t,i=q(2);w(3),Jt((t=i.currentLaunchSignal())==null?null:t.capsules)}}function Pj(e,n){if(e&1&&(g(0,"p"),C(1),v()),e&2){let t=n.$implicit;w(),Fe(" ",t," ")}}function Lj(e,n){if(e&1&&(g(0,"section")(1,"h3"),C(2,"Ship IDs involved in fairing recovery"),v(),Xt(3,Pj,2,1,"p",null,Qt),v()),e&2){let t,i=q(3);w(3),Jt((t=i.currentLaunchSignal())==null||t.fairings==null?null:t.fairings.ships)}}function jj(e,n){if(e&1&&(g(0,"h3",19),C(1,"Fairings"),v(),g(2,"section")(3,"h3"),C(4,"Fairings had been flown previously ?"),v(),g(5,"span"),C(6),v()(),g(7,"section")(8,"h3"),C(9,"Attempt was made to recover the fairings ?"),v(),g(10,"span"),C(11),v()(),g(12,"section")(13,"h3"),C(14,"Fairings were successfully recovered ?"),v(),g(15,"span"),C(16),v()(),k(17,Lj,5,0,"section")),e&2){let t,i,r,o,s=q(2);w(6),Fe(" ",!((t=s.currentLaunchSignal())==null||t.fairings==null)&&t.fairings.reused?"Yes":"No"," "),w(5),Fe(" ",!((i=s.currentLaunchSignal())==null||i.fairings==null)&&i.fairings.recovery_attempt?"Yes":"No"," "),w(5),Fe(" ",!((r=s.currentLaunchSignal())==null||r.fairings==null)&&r.fairings.recovered?"Yes":"No"," "),w(),N(!((o=s.currentLaunchSignal())==null||o.fairings==null||o.fairings.ships==null)&&o.fairings.ships.length?17:-1)}}function Vj(e,n){if(e&1&&(g(0,"tr")(1,"td"),C(2),v(),g(3,"td"),C(4),v(),g(5,"td"),C(6),v()()),e&2){let t=n.$implicit;w(2),Fe("",t.time,"s"),w(2),Re(t.altitude?t.altitude+" km":"-"),w(2),Re(t.reason)}}function Bj(e,n){if(e&1&&(g(0,"h3",19),C(1,"Failures"),v(),g(2,"section")(3,"div",18)(4,"table")(5,"thead")(6,"tr")(7,"th"),C(8,"Time After lift off"),v(),g(9,"th"),C(10,"Altitude"),v(),g(11,"th"),C(12,"Reason"),v()()(),g(13,"tbody"),Xt(14,Vj,7,3,"tr",null,Qt),v()()()()),e&2){let t,i=q(2);w(14),Jt((t=i.currentLaunchSignal())==null?null:t.failures)}}function Uj(e,n){if(e&1&&(g(0,"h3"),C(1,"Reddit launch campaign Thread"),v(),g(2,"section")(3,"a",20),C(4,"Go to Page"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null||t.links.reddit==null?null:t.links.reddit.campaign,vt)}}function Hj(e,n){if(e&1&&(g(0,"h3"),C(1,"Reddit launch discussion thread"),v(),g(2,"section")(3,"a",20),C(4,"Go to Page"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null||t.links.reddit==null?null:t.links.reddit.launch,vt)}}function $j(e,n){if(e&1&&(g(0,"h3"),C(1,"Reddit launch media thread"),v(),g(2,"section")(3,"a",20),C(4,"Go to Page"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null||t.links.reddit==null?null:t.links.reddit.media,vt)}}function zj(e,n){if(e&1&&(g(0,"h3"),C(1,"Reddit launch booster recovery discussion thread"),v(),g(2,"section")(3,"a",20),C(4,"Go to Page"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null||t.links.reddit==null?null:t.links.reddit.recovery,vt)}}function Gj(e,n){if(e&1&&(g(0,"mat-grid-tile")(1,"a",23),Ce(2,"img",24),v()()),e&2){let t=n.$implicit;w(),ye("href",t,vt),w(),ye("src",t,vt)}}function Wj(e,n){if(e&1&&(g(0,"h3"),C(1,"Flickr launch images"),v(),g(2,"section",21)(3,"mat-grid-list",22),Xt(4,Gj,3,2,"mat-grid-tile",null,Qt),v()()),e&2){let t,i=q(3);w(4),Jt((t=i.currentLaunchSignal())==null||t.links==null||t.links.flickr==null?null:t.links.flickr.original)}}function qj(e,n){if(e&1&&(g(0,"h3"),C(1,"Reddit launch booster recovery discussion thread"),v(),g(2,"section")(3,"a",20),C(4,"Go to Page"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null||t.links.reddit==null?null:t.links.reddit.recovery,vt)}}function Yj(e,n){if(e&1&&(g(0,"h3"),C(1,"Launch Mission's press kit file"),v(),g(2,"section")(3,"a",20),C(4,"Download file"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null?null:t.links.presskit,vt)}}function Zj(e,n){if(e&1&&(g(0,"h3"),C(1,"Launch Mission's YouTube webcast"),v(),g(2,"section")(3,"a",20),C(4,"Go to Youtube webcast"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null?null:t.links.webcast,vt)}}function Kj(e,n){if(e&1&&(g(0,"h3"),C(1,"Launch Mission's new article"),v(),g(2,"section")(3,"a",20),C(4,"Go to article"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null?null:t.links.article,vt)}}function Qj(e,n){if(e&1&&(g(0,"h3"),C(1,"Launch Mission's Wikipedia article"),v(),g(2,"section")(3,"a",20),C(4,"Go to Wikipedia page"),v()()),e&2){let t,i=q(3);w(3),ye("href",(t=i.currentLaunchSignal())==null||t.links==null?null:t.links.wikipedia,vt)}}function Xj(e,n){if(e&1&&(g(0,"mat-card-header")(1,"mat-card-title"),C(2,"Media information"),v()(),g(3,"mat-card-content",14),k(4,Uj,5,1),k(5,Hj,5,1),k(6,$j,5,1),k(7,zj,5,1),k(8,Wj,6,0),k(9,qj,5,1),k(10,Yj,5,1),k(11,Zj,5,1),k(12,Kj,5,1),k(13,Qj,5,1),v()),e&2){let t,i,r,o,s,a,c,l,d,u,h=q(2);w(4),N(!((t=h.currentLaunchSignal())==null||t.links==null||t.links.reddit==null)&&t.links.reddit.campaign?4:-1),w(),N(!((i=h.currentLaunchSignal())==null||i.links==null||i.links.reddit==null)&&i.links.reddit.launch?5:-1),w(),N(!((r=h.currentLaunchSignal())==null||r.links==null||r.links.reddit==null)&&r.links.reddit.media?6:-1),w(),N(!((o=h.currentLaunchSignal())==null||o.links==null||o.links.reddit==null)&&o.links.reddit.recovery?7:-1),w(),N(!((s=h.currentLaunchSignal())==null||s.links==null||s.links.flickr==null||s.links.flickr.original==null)&&s.links.flickr.original.length?8:-1),w(),N(!((a=h.currentLaunchSignal())==null||a.links==null||a.links.reddit==null)&&a.links.reddit.recovery?9:-1),w(),N(!((c=h.currentLaunchSignal())==null||c.links==null)&&c.links.presskit?10:-1),w(),N(!((l=h.currentLaunchSignal())==null||l.links==null)&&l.links.webcast?11:-1),w(),N(!((d=h.currentLaunchSignal())==null||d.links==null)&&d.links.article?12:-1),w(),N(!((u=h.currentLaunchSignal())==null||u.links==null)&&u.links.wikipedia?13:-1)}}function Jj(e,n){if(e&1&&(g(0,"mat-card",5)(1,"mat-card-header",8),Ce(2,"img",9),g(3,"mat-card-title"),C(4),v(),g(5,"mat-card-subtitle"),C(6),v(),k(7,Ej,3,0,"a",10),k(8,Cj,3,0,"a",11),v(),g(9,"div",12)(10,"mat-card",13)(11,"mat-card-header")(12,"mat-card-title"),C(13,"Core Launch Information"),v()(),g(14,"mat-card-content",14)(15,"section")(16,"h3"),C(17,"Launch date (UTC)"),v(),g(18,"span",15),C(19),jn(20,"date"),v()(),g(21,"section")(22,"h3"),C(23,"Launch date (Local time)"),v(),g(24,"span",15),C(25),jn(26,"date"),v()(),g(27,"section")(28,"h3"),C(29,"Upcoming launch ?"),v(),g(30,"span"),C(31),v()(),k(32,Dj,6,4,"section"),g(33,"section")(34,"h3"),C(35,"Was the mission successful ?"),v(),g(36,"mat-chip-set")(37,"mat-chip"),C(38),v()()(),k(39,Ij,5,1,"section"),v()(),g(40,"mat-card",13)(41,"mat-card-header")(42,"mat-card-title"),C(43,"Rocket & Core Details"),v()(),g(44,"mat-card-content",14)(45,"section")(46,"h3"),C(47,"Used Rocket ID"),v(),g(48,"span"),C(49),v()(),k(50,Sj,28,0,"section"),v()(),g(51,"mat-card",13)(52,"mat-card-header")(53,"mat-card-title"),C(54,"Payload, Crew, and Hardware"),v()(),g(55,"mat-card-content",14),k(56,Tj,5,0,"section"),k(57,Rj,5,0,"section"),k(58,Nj,5,0,"section"),k(59,Fj,5,0,"section"),g(60,"section")(61,"h3"),C(62,"ID of the launch pad used"),v(),g(63,"p"),C(64),v()()()(),g(65,"mat-card",13)(66,"mat-card-header")(67,"mat-card-title"),C(68,"Fairings & Failures"),v()(),g(69,"mat-card-content",14),k(70,jj,18,4),k(71,Bj,16,0),v()(),g(72,"mat-card",13),k(73,Xj,14,10),v()()()),e&2){let t,i,r,o,s,a,c,l,d,u,h,p,m,b,D,S,L,ue,G,fe,he,ne=q();w(2),ye("src",(t=ne.currentLaunchSignal())==null||t.links==null||t.links.patch==null?null:t.links.patch.small,vt),w(2),Re((i=ne.currentLaunchSignal())==null?null:i.name),w(2),Fe("Flight n\xB0",(r=ne.currentLaunchSignal())==null?null:r.flight_number),w(),N(ne.isFavouriteLaunch()?-1:7),w(),N(ne.isFavouriteLaunch()?8:-1),w(11),Fe(" ",Ir(20,25,(o=ne.currentLaunchSignal())==null?null:o.date_utc,"dd/MM/yyyy")," "),w(6),Fe(" ",Ir(26,28,(s=ne.currentLaunchSignal())==null?null:s.date_local,"dd/MM/yyyy")," "),w(6),Fe(" ",(a=ne.currentLaunchSignal())!=null&&a.upcoming?"Yes":"No"," "),w(),N((c=ne.currentLaunchSignal())!=null&&c.static_fire_date_utc?32:-1),w(5),te("status-success",(l=ne.currentLaunchSignal())==null?null:l.success)("status-error",!((d=ne.currentLaunchSignal())!=null&&d.success)),w(),Re((u=ne.currentLaunchSignal())!=null&&u.success?"Yes":"No"),w(),N((h=ne.currentLaunchSignal())!=null&&h.details?39:-1),w(10),Fe(" ",(p=ne.currentLaunchSignal())==null?null:p.rocket," "),w(),N(!((m=ne.currentLaunchSignal())==null||m.cores==null)&&m.cores.length?50:-1),w(6),N(!((b=ne.currentLaunchSignal())==null||b.payloads==null)&&b.payloads.length?56:-1),w(),N(!((D=ne.currentLaunchSignal())==null||D.crew==null)&&D.crew.length?57:-1),w(),N(!((S=ne.currentLaunchSignal())==null||S.ships==null)&&S.ships.length?58:-1),w(),N(!((L=ne.currentLaunchSignal())==null||L.capsules==null)&&L.capsules.length?59:-1),w(5),Re((ue=ne.currentLaunchSignal())==null?null:ue.launchpad),w(6),N((G=ne.currentLaunchSignal())!=null&&G.fairings?70:-1),w(),N(!((fe=ne.currentLaunchSignal())==null||fe.failures==null)&&fe.failures.length?71:-1),w(2),N((he=ne.currentLaunchSignal())!=null&&he.links?73:-1)}}var Sf=class e{constructor(n,t,i){this.store=n;this.activatedRoute=t;this.router=i;this.prepareCurrentLaunchSignal()}currentLaunchSignal=Te(void 0);prepareCurrentLaunchSignal(){let n=this.activatedRoute.snapshot.params.id;this.store.dispatch(Pr({launchId:n})),this.store.dispatch(qo()),this.currentLaunchSignal=Ge(()=>this.store.selectSignal(Ef)()[0]||void 0)}goBack(){this.router.navigate([""])}refreshData(){this.store.dispatch(Pr({launchId:this.currentLaunchSignal()?.id}))}addToFavorite(){this.store.dispatch(Yo({launchId:this.currentLaunchSignal()?.id}))}removeFromFavorite(){this.store.dispatch(Zo({launchId:this.currentLaunchSignal()?.id}))}isFavouriteLaunch(){return this.store.selectSignal(Cf)().includes(this.currentLaunchSignal()?.id)}static \u0275fac=function(t){return new(t||e)(ee(Lt),ee(Un),ee($i))};static \u0275cmp=K({type:e,selectors:[["app-launch-details"]],decls:13,vars:5,consts:[[1,"container-launch-details"],[1,"mat-headline-medium","title"],[1,"btns-container"],["title","Go back","matButton","outlined",1,"back-btn","circle-btn",3,"click"],["title","Refresh data","matButton","outlined",1,"circle-btn"],[1,"launch-card"],[2,"display","none"],["title","Refresh data","matButton","outlined",1,"circle-btn",3,"click"],[1,"main-card-title"],["mat-card-avatar","","alt","Mission's patch",3,"src"],["title","Add to favorites","matButton","outlined",1,"mark-favorite-btn","circle-btn"],["title","Remove from favorites","matButton","outlined",1,"mark-favorite-btn","circle-btn"],[1,"mat-card-content-wrapper"],[1,"custom-card"],[1,"card-content"],[1,"date-text"],["title","Add to favorites","matButton","outlined",1,"mark-favorite-btn","circle-btn",3,"click"],["title","Remove from favorites","matButton","outlined",1,"mark-favorite-btn","circle-btn",3,"click"],[1,"table-wrapper"],[1,"section-title"],["matButton","outlined","target","_blank",3,"href"],[1,"section-flickr"],["cols","4","rowHeight","1:1"],["target","_blank",3,"href"],[3,"src"]],template:function(t,i){t&1&&(g(0,"div",0)(1,"h1",1)(2,"span"),C(3,"SpaceX Launch Details"),v(),g(4,"div",2)(5,"a",3),pe("click",function(){return i.goBack()}),g(6,"mat-icon"),C(7,"keyboard_backspace"),v()(),k(8,wj,3,0,"a",4),v()(),k(9,Jj,74,31,"mat-card",5),v(),g(10,"pre",6),C(11),jn(12,"json"),v()),t&2&&(w(8),N(i.currentLaunchSignal()?8:-1),w(),N(i.currentLaunchSignal()?9:-1),w(2),Fe("    ",aa(12,3,i.currentLaunchSignal()),`
`))},dependencies:[Io,rf,uf,of,df,af,lf,cf,sf,zr,wf,bf,_f,vf,vc,gf,cc,rs,Cx,Ex,oy,tg,ga],styles:[".container-launch-details[_ngcontent-%COMP%]{max-width:1600px;margin:0 auto;height:calc(100% - 48px);display:flex;flex-direction:column;padding:24px;overflow:hidden}[_nghost-%COMP%]     .mat-mdc-card-header-text{flex-grow:1}.status-success[class][_ngcontent-%COMP%]{background-color:#e2fbe8;color:#1e7e34}.status-error[class][_ngcontent-%COMP%]{background-color:#fde8e8;color:#c82333}h1.title[_ngcontent-%COMP%]{margin-bottom:24px;color:#1e293b}section[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{display:flex;align-items:center;margin-bottom:5px}.launch-details-wrapper[_ngcontent-%COMP%]{height:0px;flex-grow:1;overflow:hidden}mat-card.launch-card[_ngcontent-%COMP%]{height:0px;flex-grow:1;overflow:hidden;display:flex;flex-direction:column}mat-card.launch-card[_ngcontent-%COMP%]   .mat-card-content-wrapper[_ngcontent-%COMP%]{height:0px;flex-grow:1;overflow:auto}.main-card-title[_ngcontent-%COMP%]{box-shadow:0 0 3px 1px gray;position:relative;z-index:1;background-color:#fff}table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%], table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{border:1px solid gray;text-align:center}table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{padding:2px}table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{padding:3px}table[_ngcontent-%COMP%]{border-collapse:collapse;width:100%;min-width:600px;margin:-1px}table[_ngcontent-%COMP%]   tr[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]:last-child, table[_ngcontent-%COMP%]   tr[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]:last-child{border-right:none}mat-card-title[_ngcontent-%COMP%]{font-weight:bolder}.mat-card-content-wrapper[_ngcontent-%COMP%]   mat-card-title[_ngcontent-%COMP%]{font-weight:700!important}.mat-card-content-wrapper[_ngcontent-%COMP%]   .section-title[_ngcontent-%COMP%]{font-size:1.23rem}.table-wrapper[_ngcontent-%COMP%]{overflow:hidden;border:1px solid gray;border-radius:4px}.section-flickr[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{border-radius:5px!important;overflow:hidden;width:100%;height:100%}.section-flickr[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{height:100%;width:100%}.section-flickr[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover   img[_ngcontent-%COMP%]{transform:scale(1.1)}.custom-card[_ngcontent-%COMP%]{background-color:#fff;border-radius:0}.custom-card[_ngcontent-%COMP%]:first-child ~ .custom-card[_ngcontent-%COMP%]{margin-top:30px}.custom-card[_ngcontent-%COMP%]   mat-card-content[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{text-transform:capitalize}.back-btn[_ngcontent-%COMP%]{margin-right:5px}"]})};var Dx=[{path:"",component:If,pathMatch:"full"},{path:"launch/:id",component:Sf}];var Ix={providers:[Hg(Dx),mg(),YC({launch:xD}),tD([Bu]),DD({maxAge:25,logOnly:!1})]};function eV(e,n){e&1&&At(0,"div",2)}var tV=new y("MAT_PROGRESS_BAR_DEFAULT_OPTIONS");var Sx=(()=>{class e{_elementRef=f(Q);_ngZone=f(j);_changeDetectorRef=f(ht);_renderer=f(it);_cleanupTransitionEnd;constructor(){let t=Mv(),i=f(tV,{optional:!0});this._isNoopAnimation=t==="di-disabled",t==="reduced-motion"&&this._elementRef.nativeElement.classList.add("mat-progress-bar-reduced-motion"),i&&(i.color&&(this.color=this._defaultColor=i.color),this.mode=i.mode||this.mode)}_isNoopAnimation;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;_defaultColor="primary";get value(){return this._value}set value(t){this._value=xx(t||0),this._changeDetectorRef.markForCheck()}_value=0;get bufferValue(){return this._bufferValue||0}set bufferValue(t){this._bufferValue=xx(t||0),this._changeDetectorRef.markForCheck()}_bufferValue=0;animationEnd=new oe;get mode(){return this._mode}set mode(t){this._mode=t,this._changeDetectorRef.markForCheck()}_mode="determinate";ngAfterViewInit(){this._ngZone.runOutsideAngular(()=>{this._cleanupTransitionEnd=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._transitionendHandler)})}ngOnDestroy(){this._cleanupTransitionEnd?.()}_getPrimaryBarTransform(){return`scaleX(${this._isIndeterminate()?1:this.value/100})`}_getBufferBarFlexBasis(){return`${this.mode==="buffer"?this.bufferValue:100}%`}_isIndeterminate(){return this.mode==="indeterminate"||this.mode==="query"}_transitionendHandler=t=>{this.animationEnd.observers.length===0||!t.target||!t.target.classList.contains("mdc-linear-progress__primary-bar")||(this.mode==="determinate"||this.mode==="buffer")&&this._ngZone.run(()=>this.animationEnd.next({value:this.value}))};static \u0275fac=function(i){return new(i||e)};static \u0275cmp=K({type:e,selectors:[["mat-progress-bar"]],hostAttrs:["role","progressbar","aria-valuemin","0","aria-valuemax","100","tabindex","-1",1,"mat-mdc-progress-bar","mdc-linear-progress"],hostVars:10,hostBindings:function(i,r){i&2&&(be("aria-valuenow",r._isIndeterminate()?null:r.value)("mode",r.mode),wn("mat-"+r.color),te("_mat-animation-noopable",r._isNoopAnimation)("mdc-linear-progress--animation-ready",!r._isNoopAnimation)("mdc-linear-progress--indeterminate",r._isIndeterminate()))},inputs:{color:"color",value:[2,"value","value",Vn],bufferValue:[2,"bufferValue","bufferValue",Vn],mode:"mode"},outputs:{animationEnd:"animationEnd"},exportAs:["matProgressBar"],decls:7,vars:5,consts:[["aria-hidden","true",1,"mdc-linear-progress__buffer"],[1,"mdc-linear-progress__buffer-bar"],[1,"mdc-linear-progress__buffer-dots"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__primary-bar"],[1,"mdc-linear-progress__bar-inner"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__secondary-bar"]],template:function(i,r){i&1&&(rt(0,"div",0),At(1,"div",1),k(2,eV,1,0,"div",2),ot(),rt(3,"div",3),At(4,"span",4),ot(),rt(5,"div",5),At(6,"span",4),ot()),i&2&&(w(),ra("flex-basis",r._getBufferBarFlexBasis()),w(),N(r.mode==="buffer"?2:-1),w(),ra("transform",r._getPrimaryBarTransform()))},styles:[`.mat-mdc-progress-bar {
  --mat-progress-bar-animation-multiplier: 1;
  display: block;
  text-align: start;
}
.mat-mdc-progress-bar[mode=query] {
  transform: scaleX(-1);
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-dots,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__secondary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__bar-inner.mdc-linear-progress__bar-inner {
  animation: none;
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-bar {
  transition: transform 1ms;
}

.mat-progress-bar-reduced-motion {
  --mat-progress-bar-animation-multiplier: 2;
}

.mdc-linear-progress {
  position: relative;
  width: 100%;
  transform: translateZ(0);
  outline: 1px solid transparent;
  overflow-x: hidden;
  transition: opacity 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: max(var(--mat-progress-bar-track-height, 4px), var(--mat-progress-bar-active-indicator-height, 4px));
}
@media (forced-colors: active) {
  .mdc-linear-progress {
    outline-color: CanvasText;
  }
}

.mdc-linear-progress__bar {
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  animation: none;
  transform-origin: top left;
  transition: transform 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: var(--mat-progress-bar-active-indicator-height, 4px);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__bar {
  transition: none;
}
[dir=rtl] .mdc-linear-progress__bar {
  right: 0;
  transform-origin: center right;
}

.mdc-linear-progress__bar-inner {
  display: inline-block;
  position: absolute;
  width: 100%;
  animation: none;
  border-top-style: solid;
  border-color: var(--mat-progress-bar-active-indicator-color, var(--mat-sys-primary));
  border-top-width: var(--mat-progress-bar-active-indicator-height, 4px);
}

.mdc-linear-progress__buffer {
  display: flex;
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  overflow: hidden;
  height: var(--mat-progress-bar-track-height, 4px);
  border-radius: var(--mat-progress-bar-track-shape, var(--mat-sys-corner-none));
}

.mdc-linear-progress__buffer-dots {
  background-image: radial-gradient(circle, var(--mat-progress-bar-track-color, var(--mat-sys-surface-variant)) calc(var(--mat-progress-bar-track-height, 4px) / 2), transparent 0);
  background-repeat: repeat-x;
  background-size: calc(calc(var(--mat-progress-bar-track-height, 4px) / 2) * 5);
  background-position: left;
  flex: auto;
  transform: rotate(180deg);
  animation: mdc-linear-progress-buffering calc(250ms * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
@media (forced-colors: active) {
  .mdc-linear-progress__buffer-dots {
    background-color: ButtonBorder;
  }
}
[dir=rtl] .mdc-linear-progress__buffer-dots {
  animation: mdc-linear-progress-buffering-reverse calc(250ms * var(--mat-progress-bar-animation-multiplier)) infinite linear;
  transform: rotate(0);
}

.mdc-linear-progress__buffer-bar {
  flex: 0 1 100%;
  transition: flex-basis 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  background-color: var(--mat-progress-bar-track-color, var(--mat-sys-surface-variant));
}

.mdc-linear-progress__primary-bar {
  transform: scaleX(0);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  left: -145.166611%;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation: mdc-linear-progress-primary-indeterminate-translate calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-primary-indeterminate-scale calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation-name: mdc-linear-progress-primary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  right: -145.166611%;
  left: auto;
}

.mdc-linear-progress__secondary-bar {
  display: none;
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  left: -54.888891%;
  display: block;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation: mdc-linear-progress-secondary-indeterminate-translate calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-secondary-indeterminate-scale calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation-name: mdc-linear-progress-secondary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  right: -54.888891%;
  left: auto;
}

@keyframes mdc-linear-progress-buffering {
  from {
    transform: rotate(180deg) translateX(calc(var(--mat-progress-bar-track-height, 4px) * -2.5));
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(83.67142%);
  }
  100% {
    transform: translateX(200.611057%);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-scale {
  0% {
    transform: scaleX(0.08);
  }
  36.65% {
    animation-timing-function: cubic-bezier(0.334731, 0.12482, 0.785844, 1);
    transform: scaleX(0.08);
  }
  69.15% {
    animation-timing-function: cubic-bezier(0.06, 0.11, 0.6, 1);
    transform: scaleX(0.661479);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(84.386165%);
  }
  100% {
    transform: translateX(160.277782%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-scale {
  0% {
    animation-timing-function: cubic-bezier(0.205028, 0.057051, 0.57661, 0.453971);
    transform: scaleX(0.08);
  }
  19.15% {
    animation-timing-function: cubic-bezier(0.152313, 0.196432, 0.648374, 1.004315);
    transform: scaleX(0.457104);
  }
  44.15% {
    animation-timing-function: cubic-bezier(0.257759, -0.003163, 0.211762, 1.38179);
    transform: scaleX(0.72796);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate-reverse {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(-83.67142%);
  }
  100% {
    transform: translateX(-200.611057%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate-reverse {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(-37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(-84.386165%);
  }
  100% {
    transform: translateX(-160.277782%);
  }
}
@keyframes mdc-linear-progress-buffering-reverse {
  from {
    transform: translateX(-10px);
  }
}
`],encapsulation:2,changeDetection:0})}return e})();function xx(e,n=0,t=100){return Math.max(n,Math.min(t,e))}var Mx=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=X({type:e});static \u0275inj=Z({imports:[Ue]})}return e})();function iV(e,n){e&1&&Ce(0,"mat-progress-bar",0)}var Mf=class e{constructor(n){this.store=n;this.isLoading$=this.store.select(t=>t.launch).pipe(T(t=>t.loading))}isLoading$;static \u0275fac=function(t){return new(t||e)(ee(Lt))};static \u0275cmp=K({type:e,selectors:[["app-root"]],decls:3,vars:3,consts:[["mode","indeterminate"]],template:function(t,i){t&1&&(k(0,iV,1,0,"mat-progress-bar",0),jn(1,"async"),Ce(2,"router-outlet")),t&2&&N(aa(1,1,i.isLoading$)?0:-1)},dependencies:[Va,Mx,Sx,eg],styles:["mat-progress-bar[_ngcontent-%COMP%]{position:fixed;top:0}"]})};dg(Mf,Ix).catch(e=>console.error(e));
