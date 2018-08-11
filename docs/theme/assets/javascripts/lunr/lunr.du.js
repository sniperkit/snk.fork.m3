/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

!function(r,e){"function"==typeof define&&define.amd?define(e):"object"==typeof exports?module.exports=e():e()(r.lunr)}(this,function(){return function(r){if(void 0===r)throw new Error("Lunr is not present. Please include / require Lunr before this script.");if(void 0===r.stemmerSupport)throw new Error("Lunr stemmer support is not present. Please include / require Lunr stemmer support before this script.");r.du=function(){this.pipeline.reset(),this.pipeline.add(r.du.trimmer,r.du.stopWordFilter,r.du.stemmer),this.searchPipeline&&(this.searchPipeline.reset(),this.searchPipeline.add(r.du.stemmer))},r.du.wordCharacters="A-Za-zªºÀ-ÖØ-öø-ʸˠ-ˤᴀ-ᴥᴬ-ᵜᵢ-ᵥᵫ-ᵷᵹ-ᶾḀ-ỿⁱⁿₐ-ₜKÅℲⅎⅠ-ↈⱠ-ⱿꜢ-ꞇꞋ-ꞭꞰ-ꞷꟷ-ꟿꬰ-ꭚꭜ-ꭤﬀ-ﬆＡ-Ｚａ-ｚ",r.du.trimmer=r.trimmerSupport.generateTrimmer(r.du.wordCharacters),r.Pipeline.registerFunction(r.du.trimmer,"trimmer-du"),r.du.stemmer=function(){var e=r.stemmerSupport.Among,i=r.stemmerSupport.SnowballProgram,n=new function(){function r(){for(var r,e,i,o=C.cursor;;){if(C.bra=C.cursor,r=C.find_among(b,11))switch(C.ket=C.cursor,r){case 1:C.slice_from("a");continue;case 2:C.slice_from("e");continue;case 3:C.slice_from("i");continue;case 4:C.slice_from("o");continue;case 5:C.slice_from("u");continue;case 6:if(C.cursor>=C.limit)break;C.cursor++;continue}break}for(C.cursor=o,C.bra=o,C.eq_s(1,"y")?(C.ket=C.cursor,C.slice_from("Y")):C.cursor=o;;)if(e=C.cursor,C.in_grouping(q,97,232)){if(i=C.cursor,C.bra=i,C.eq_s(1,"i"))C.ket=C.cursor,C.in_grouping(q,97,232)&&(C.slice_from("I"),C.cursor=e);else if(C.cursor=i,C.eq_s(1,"y"))C.ket=C.cursor,C.slice_from("Y"),C.cursor=e;else if(n(e))break}else if(n(e))break}function n(r){return C.cursor=r,r>=C.limit||(C.cursor++,!1)}function o(){_=C.limit,f=_,t()||((_=C.cursor)<3&&(_=3),t()||(f=C.cursor))}function t(){for(;!C.in_grouping(q,97,232);){if(C.cursor>=C.limit)return!0;C.cursor++}for(;!C.out_grouping(q,97,232);){if(C.cursor>=C.limit)return!0;C.cursor++}return!1}function s(){for(var r;;)if(C.bra=C.cursor,r=C.find_among(p,3))switch(C.ket=C.cursor,r){case 1:C.slice_from("y");break;case 2:C.slice_from("i");break;case 3:if(C.cursor>=C.limit)return;C.cursor++}}function u(){return _<=C.cursor}function c(){return f<=C.cursor}function a(){var r=C.limit-C.cursor;C.find_among_b(g,3)&&(C.cursor=C.limit-r,C.ket=C.cursor,C.cursor>C.limit_backward&&(C.cursor--,C.bra=C.cursor,C.slice_del()))}function l(){var r;w=!1,C.ket=C.cursor,C.eq_s_b(1,"e")&&(C.bra=C.cursor,u()&&(r=C.limit-C.cursor,C.out_grouping_b(q,97,232)&&(C.cursor=C.limit-r,C.slice_del(),w=!0,a())))}function m(){var r;u()&&(r=C.limit-C.cursor,C.out_grouping_b(q,97,232)&&(C.cursor=C.limit-r,C.eq_s_b(3,"gem")||(C.cursor=C.limit-r,C.slice_del(),a())))}function d(){var r,e,i,n,o,t,s=C.limit-C.cursor;if(C.ket=C.cursor,r=C.find_among_b(h,5))switch(C.bra=C.cursor,r){case 1:u()&&C.slice_from("heid");break;case 2:m();break;case 3:u()&&C.out_grouping_b(j,97,232)&&C.slice_del()}if(C.cursor=C.limit-s,l(),C.cursor=C.limit-s,C.ket=C.cursor,C.eq_s_b(4,"heid")&&(C.bra=C.cursor,c()&&(e=C.limit-C.cursor,C.eq_s_b(1,"c")||(C.cursor=C.limit-e,C.slice_del(),C.ket=C.cursor,C.eq_s_b(2,"en")&&(C.bra=C.cursor,m())))),C.cursor=C.limit-s,C.ket=C.cursor,r=C.find_among_b(k,6))switch(C.bra=C.cursor,r){case 1:if(c()){if(C.slice_del(),i=C.limit-C.cursor,C.ket=C.cursor,C.eq_s_b(2,"ig")&&(C.bra=C.cursor,c()&&(n=C.limit-C.cursor,!C.eq_s_b(1,"e")))){C.cursor=C.limit-n,C.slice_del();break}C.cursor=C.limit-i,a()}break;case 2:c()&&(o=C.limit-C.cursor,C.eq_s_b(1,"e")||(C.cursor=C.limit-o,C.slice_del()));break;case 3:c()&&(C.slice_del(),l());break;case 4:c()&&C.slice_del();break;case 5:c()&&w&&C.slice_del()}C.cursor=C.limit-s,C.out_grouping_b(z,73,232)&&(t=C.limit-C.cursor,C.find_among_b(v,4)&&C.out_grouping_b(q,97,232)&&(C.cursor=C.limit-t,C.ket=C.cursor,C.cursor>C.limit_backward&&(C.cursor--,C.bra=C.cursor,C.slice_del())))}var f,_,w,b=[new e("",-1,6),new e("á",0,1),new e("ä",0,1),new e("é",0,2),new e("ë",0,2),new e("í",0,3),new e("ï",0,3),new e("ó",0,4),new e("ö",0,4),new e("ú",0,5),new e("ü",0,5)],p=[new e("",-1,3),new e("I",0,2),new e("Y",0,1)],g=[new e("dd",-1,-1),new e("kk",-1,-1),new e("tt",-1,-1)],h=[new e("ene",-1,2),new e("se",-1,3),new e("en",-1,2),new e("heden",2,1),new e("s",-1,3)],k=[new e("end",-1,1),new e("ig",-1,2),new e("ing",-1,1),new e("lijk",-1,3),new e("baar",-1,4),new e("bar",-1,5)],v=[new e("aa",-1,-1),new e("ee",-1,-1),new e("oo",-1,-1),new e("uu",-1,-1)],q=[17,65,16,1,0,0,0,0,0,0,0,0,0,0,0,0,128],z=[1,0,0,17,65,16,1,0,0,0,0,0,0,0,0,0,0,0,0,128],j=[17,67,16,1,0,0,0,0,0,0,0,0,0,0,0,0,128],C=new i;this.setCurrent=function(r){C.setCurrent(r)},this.getCurrent=function(){return C.getCurrent()},this.stem=function(){var e=C.cursor;return r(),C.cursor=e,o(),C.limit_backward=e,C.cursor=C.limit,d(),C.cursor=C.limit_backward,s(),!0}};return function(r){return"function"==typeof r.update?r.update(function(r){return n.setCurrent(r),n.stem(),n.getCurrent()}):(n.setCurrent(r),n.stem(),n.getCurrent())}}(),r.Pipeline.registerFunction(r.du.stemmer,"stemmer-du"),r.du.stopWordFilter=r.generateStopWordFilter(" aan al alles als altijd andere ben bij daar dan dat de der deze die dit doch doen door dus een eens en er ge geen geweest haar had heb hebben heeft hem het hier hij hoe hun iemand iets ik in is ja je kan kon kunnen maar me meer men met mij mijn moet na naar niet niets nog nu of om omdat onder ons ook op over reeds te tegen toch toen tot u uit uw van veel voor want waren was wat werd wezen wie wil worden wordt zal ze zelf zich zij zijn zo zonder zou".split(" ")),r.Pipeline.registerFunction(r.du.stopWordFilter,"stopWordFilter-du")}});