/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

!function(e,i){"function"==typeof define&&define.amd?define(i):"object"==typeof exports?module.exports=i():i()(e.lunr)}(this,function(){return function(e){e.multiLanguage=function(){for(var i=Array.prototype.slice.call(arguments),t=i.join("-"),r="",n=[],s=[],p=0;p<i.length;++p)"en"==i[p]?(r+="\\w",n.unshift(e.stopWordFilter),n.push(e.stemmer),s.push(e.stemmer)):(r+=e[i[p]].wordCharacters,n.unshift(e[i[p]].stopWordFilter),n.push(e[i[p]].stemmer),s.push(e[i[p]].stemmer));var o=e.trimmerSupport.generateTrimmer(r);return e.Pipeline.registerFunction(o,"lunr-multi-trimmer-"+t),n.unshift(o),function(){this.pipeline.reset(),this.pipeline.add.apply(this.pipeline,n),this.searchPipeline&&(this.searchPipeline.reset(),this.searchPipeline.add.apply(this.searchPipeline,s))}}}});