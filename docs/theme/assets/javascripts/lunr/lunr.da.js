/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

!function(e,r){"function"==typeof define&&define.amd?define(r):"object"==typeof exports?module.exports=r():r()(e.lunr)}(this,function(){return function(e){if(void 0===e)throw new Error("Lunr is not present. Please include / require Lunr before this script.");if(void 0===e.stemmerSupport)throw new Error("Lunr stemmer support is not present. Please include / require Lunr stemmer support before this script.");e.da=function(){this.pipeline.reset(),this.pipeline.add(e.da.trimmer,e.da.stopWordFilter,e.da.stemmer),this.searchPipeline&&(this.searchPipeline.reset(),this.searchPipeline.add(e.da.stemmer))},e.da.wordCharacters="A-Za-zªºÀ-ÖØ-öø-ʸˠ-ˤᴀ-ᴥᴬ-ᵜᵢ-ᵥᵫ-ᵷᵹ-ᶾḀ-ỿⁱⁿₐ-ₜKÅℲⅎⅠ-ↈⱠ-ⱿꜢ-ꞇꞋ-ꞭꞰ-ꞷꟷ-ꟿꬰ-ꭚꭜ-ꭤﬀ-ﬆＡ-Ｚａ-ｚ",e.da.trimmer=e.trimmerSupport.generateTrimmer(e.da.wordCharacters),e.Pipeline.registerFunction(e.da.trimmer,"trimmer-da"),e.da.stemmer=function(){var r=e.stemmerSupport.Among,i=e.stemmerSupport.SnowballProgram,n=new function(){function e(){var e,r=f.cursor+3;if(d=f.limit,0<=r&&r<=f.limit){for(a=r;;){if(e=f.cursor,f.in_grouping(w,97,248)){f.cursor=e;break}if(f.cursor=e,e>=f.limit)return;f.cursor++}for(;!f.out_grouping(w,97,248);){if(f.cursor>=f.limit)return;f.cursor++}(d=f.cursor)<a&&(d=a)}}function n(){var e,r;if(f.cursor>=d&&(r=f.limit_backward,f.limit_backward=d,f.ket=f.cursor,e=f.find_among_b(c,32),f.limit_backward=r,e))switch(f.bra=f.cursor,e){case 1:f.slice_del();break;case 2:f.in_grouping_b(p,97,229)&&f.slice_del()}}function t(){var e,r=f.limit-f.cursor;f.cursor>=d&&(e=f.limit_backward,f.limit_backward=d,f.ket=f.cursor,f.find_among_b(l,4)?(f.bra=f.cursor,f.limit_backward=e,f.cursor=f.limit-r,f.cursor>f.limit_backward&&(f.cursor--,f.bra=f.cursor,f.slice_del())):f.limit_backward=e)}function s(){var e,r,i,n=f.limit-f.cursor;if(f.ket=f.cursor,f.eq_s_b(2,"st")&&(f.bra=f.cursor,f.eq_s_b(2,"ig")&&f.slice_del()),f.cursor=f.limit-n,f.cursor>=d&&(r=f.limit_backward,f.limit_backward=d,f.ket=f.cursor,e=f.find_among_b(m,5),f.limit_backward=r,e))switch(f.bra=f.cursor,e){case 1:f.slice_del(),i=f.limit-f.cursor,t(),f.cursor=f.limit-i;break;case 2:f.slice_from("løs")}}function o(){var e;f.cursor>=d&&(e=f.limit_backward,f.limit_backward=d,f.ket=f.cursor,f.out_grouping_b(w,97,248)?(f.bra=f.cursor,u=f.slice_to(u),f.limit_backward=e,f.eq_v_b(u)&&f.slice_del()):f.limit_backward=e)}var a,d,u,c=[new r("hed",-1,1),new r("ethed",0,1),new r("ered",-1,1),new r("e",-1,1),new r("erede",3,1),new r("ende",3,1),new r("erende",5,1),new r("ene",3,1),new r("erne",3,1),new r("ere",3,1),new r("en",-1,1),new r("heden",10,1),new r("eren",10,1),new r("er",-1,1),new r("heder",13,1),new r("erer",13,1),new r("s",-1,2),new r("heds",16,1),new r("es",16,1),new r("endes",18,1),new r("erendes",19,1),new r("enes",18,1),new r("ernes",18,1),new r("eres",18,1),new r("ens",16,1),new r("hedens",24,1),new r("erens",24,1),new r("ers",16,1),new r("ets",16,1),new r("erets",28,1),new r("et",-1,1),new r("eret",30,1)],l=[new r("gd",-1,-1),new r("dt",-1,-1),new r("gt",-1,-1),new r("kt",-1,-1)],m=[new r("ig",-1,1),new r("lig",0,1),new r("elig",1,1),new r("els",-1,1),new r("løst",-1,2)],w=[17,65,16,1,0,0,0,0,0,0,0,0,0,0,0,0,48,0,128],p=[239,254,42,3,0,0,0,0,0,0,0,0,0,0,0,0,16],f=new i;this.setCurrent=function(e){f.setCurrent(e)},this.getCurrent=function(){return f.getCurrent()},this.stem=function(){var r=f.cursor;return e(),f.limit_backward=r,f.cursor=f.limit,n(),f.cursor=f.limit,t(),f.cursor=f.limit,s(),f.cursor=f.limit,o(),!0}};return function(e){return"function"==typeof e.update?e.update(function(e){return n.setCurrent(e),n.stem(),n.getCurrent()}):(n.setCurrent(e),n.stem(),n.getCurrent())}}(),e.Pipeline.registerFunction(e.da.stemmer,"stemmer-da"),e.da.stopWordFilter=e.generateStopWordFilter("ad af alle alt anden at blev blive bliver da de dem den denne der deres det dette dig din disse dog du efter eller en end er et for fra ham han hans har havde have hende hendes her hos hun hvad hvis hvor i ikke ind jeg jer jo kunne man mange med meget men mig min mine mit mod ned noget nogle nu når og også om op os over på selv sig sin sine sit skal skulle som sådan thi til ud under var vi vil ville vor være været".split(" ")),e.Pipeline.registerFunction(e.da.stopWordFilter,"stopWordFilter-da")}});