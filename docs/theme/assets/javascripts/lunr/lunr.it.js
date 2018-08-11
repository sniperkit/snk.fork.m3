/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

!function(e,r){"function"==typeof define&&define.amd?define(r):"object"==typeof exports?module.exports=r():r()(e.lunr)}(this,function(){return function(e){if(void 0===e)throw new Error("Lunr is not present. Please include / require Lunr before this script.");if(void 0===e.stemmerSupport)throw new Error("Lunr stemmer support is not present. Please include / require Lunr stemmer support before this script.");e.it=function(){this.pipeline.reset(),this.pipeline.add(e.it.trimmer,e.it.stopWordFilter,e.it.stemmer),this.searchPipeline&&(this.searchPipeline.reset(),this.searchPipeline.add(e.it.stemmer))},e.it.wordCharacters="A-Za-zªºÀ-ÖØ-öø-ʸˠ-ˤᴀ-ᴥᴬ-ᵜᵢ-ᵥᵫ-ᵷᵹ-ᶾḀ-ỿⁱⁿₐ-ₜKÅℲⅎⅠ-ↈⱠ-ⱿꜢ-ꞇꞋ-ꞭꞰ-ꞷꟷ-ꟿꬰ-ꭚꭜ-ꭤﬀ-ﬆＡ-Ｚａ-ｚ",e.it.trimmer=e.trimmerSupport.generateTrimmer(e.it.wordCharacters),e.Pipeline.registerFunction(e.it.trimmer,"trimmer-it"),e.it.stemmer=function(){var r=e.stemmerSupport.Among,n=e.stemmerSupport.SnowballProgram,i=new function(){function e(e,r,n){return!(!x.eq_s(1,e)||(x.ket=x.cursor,!x.in_grouping(L,97,249)))&&(x.slice_from(r),x.cursor=n,!0)}function i(){for(var r,n,i,o,t=x.cursor;;){if(x.bra=x.cursor,r=x.find_among(h,7))switch(x.ket=x.cursor,r){case 1:x.slice_from("à");continue;case 2:x.slice_from("è");continue;case 3:x.slice_from("ì");continue;case 4:x.slice_from("ò");continue;case 5:x.slice_from("ù");continue;case 6:x.slice_from("qU");continue;case 7:if(x.cursor>=x.limit)break;x.cursor++;continue}break}for(x.cursor=t;;)for(n=x.cursor;;){if(i=x.cursor,x.in_grouping(L,97,249)){if(x.bra=x.cursor,o=x.cursor,e("u","U",i))break;if(x.cursor=o,e("i","I",i))break}if(x.cursor=i,x.cursor>=x.limit)return void(x.cursor=n);x.cursor++}}function o(e){if(x.cursor=e,!x.in_grouping(L,97,249))return!1;for(;!x.out_grouping(L,97,249);){if(x.cursor>=x.limit)return!1;x.cursor++}return!0}function t(){if(x.in_grouping(L,97,249)){var e=x.cursor;if(x.out_grouping(L,97,249)){for(;!x.in_grouping(L,97,249);){if(x.cursor>=x.limit)return o(e);x.cursor++}return!0}return o(e)}return!1}function s(){var e,r=x.cursor;if(!t()){if(x.cursor=r,!x.out_grouping(L,97,249))return;if(e=x.cursor,x.out_grouping(L,97,249)){for(;!x.in_grouping(L,97,249);){if(x.cursor>=x.limit)return x.cursor=e,void(x.in_grouping(L,97,249)&&x.cursor<x.limit&&x.cursor++);x.cursor++}return void(k=x.cursor)}if(x.cursor=e,!x.in_grouping(L,97,249)||x.cursor>=x.limit)return;x.cursor++}k=x.cursor}function a(){for(;!x.in_grouping(L,97,249);){if(x.cursor>=x.limit)return!1;x.cursor++}for(;!x.out_grouping(L,97,249);){if(x.cursor>=x.limit)return!1;x.cursor++}return!0}function u(){var e=x.cursor;k=x.limit,p=k,g=k,s(),x.cursor=e,a()&&(p=x.cursor,a()&&(g=x.cursor))}function c(){for(var e;;){if(x.bra=x.cursor,!(e=x.find_among(q,3)))break;switch(x.ket=x.cursor,e){case 1:x.slice_from("i");break;case 2:x.slice_from("u");break;case 3:if(x.cursor>=x.limit)return;x.cursor++}}}function w(){return k<=x.cursor}function l(){return p<=x.cursor}function m(){return g<=x.cursor}function f(){var e;if(x.ket=x.cursor,x.find_among_b(C,37)&&(x.bra=x.cursor,(e=x.find_among_b(z,5))&&w()))switch(e){case 1:x.slice_del();break;case 2:x.slice_from("e")}}function v(){var e;if(x.ket=x.cursor,!(e=x.find_among_b(S,51)))return!1;switch(x.bra=x.cursor,e){case 1:if(!m())return!1;x.slice_del();break;case 2:if(!m())return!1;x.slice_del(),x.ket=x.cursor,x.eq_s_b(2,"ic")&&(x.bra=x.cursor,m()&&x.slice_del());break;case 3:if(!m())return!1;x.slice_from("log");break;case 4:if(!m())return!1;x.slice_from("u");break;case 5:if(!m())return!1;x.slice_from("ente");break;case 6:if(!w())return!1;x.slice_del();break;case 7:if(!l())return!1;x.slice_del(),x.ket=x.cursor,(e=x.find_among_b(P,4))&&(x.bra=x.cursor,m()&&(x.slice_del(),1==e&&(x.ket=x.cursor,x.eq_s_b(2,"at")&&(x.bra=x.cursor,m()&&x.slice_del()))));break;case 8:if(!m())return!1;x.slice_del(),x.ket=x.cursor,(e=x.find_among_b(F,3))&&(x.bra=x.cursor,1==e&&m()&&x.slice_del());break;case 9:if(!m())return!1;x.slice_del(),x.ket=x.cursor,x.eq_s_b(2,"at")&&(x.bra=x.cursor,m()&&(x.slice_del(),x.ket=x.cursor,x.eq_s_b(2,"ic")&&(x.bra=x.cursor,m()&&x.slice_del())))}return!0}function b(){var e,r;x.cursor>=k&&(r=x.limit_backward,x.limit_backward=k,x.ket=x.cursor,(e=x.find_among_b(W,87))&&(x.bra=x.cursor,1==e&&x.slice_del()),x.limit_backward=r)}function d(){var e=x.limit-x.cursor;x.ket=x.cursor,x.in_grouping_b(y,97,242)&&(x.bra=x.cursor,w()&&(x.slice_del(),x.ket=x.cursor,x.eq_s_b(1,"i")&&(x.bra=x.cursor,w())))?x.slice_del():x.cursor=x.limit-e}function _(){d(),x.ket=x.cursor,x.eq_s_b(1,"h")&&(x.bra=x.cursor,x.in_grouping_b(U,99,103)&&w()&&x.slice_del())}var g,p,k,h=[new r("",-1,7),new r("qu",0,6),new r("á",0,1),new r("é",0,2),new r("í",0,3),new r("ó",0,4),new r("ú",0,5)],q=[new r("",-1,3),new r("I",0,1),new r("U",0,2)],C=[new r("la",-1,-1),new r("cela",0,-1),new r("gliela",0,-1),new r("mela",0,-1),new r("tela",0,-1),new r("vela",0,-1),new r("le",-1,-1),new r("cele",6,-1),new r("gliele",6,-1),new r("mele",6,-1),new r("tele",6,-1),new r("vele",6,-1),new r("ne",-1,-1),new r("cene",12,-1),new r("gliene",12,-1),new r("mene",12,-1),new r("sene",12,-1),new r("tene",12,-1),new r("vene",12,-1),new r("ci",-1,-1),new r("li",-1,-1),new r("celi",20,-1),new r("glieli",20,-1),new r("meli",20,-1),new r("teli",20,-1),new r("veli",20,-1),new r("gli",20,-1),new r("mi",-1,-1),new r("si",-1,-1),new r("ti",-1,-1),new r("vi",-1,-1),new r("lo",-1,-1),new r("celo",31,-1),new r("glielo",31,-1),new r("melo",31,-1),new r("telo",31,-1),new r("velo",31,-1)],z=[new r("ando",-1,1),new r("endo",-1,1),new r("ar",-1,2),new r("er",-1,2),new r("ir",-1,2)],P=[new r("ic",-1,-1),new r("abil",-1,-1),new r("os",-1,-1),new r("iv",-1,1)],F=[new r("ic",-1,1),new r("abil",-1,1),new r("iv",-1,1)],S=[new r("ica",-1,1),new r("logia",-1,3),new r("osa",-1,1),new r("ista",-1,1),new r("iva",-1,9),new r("anza",-1,1),new r("enza",-1,5),new r("ice",-1,1),new r("atrice",7,1),new r("iche",-1,1),new r("logie",-1,3),new r("abile",-1,1),new r("ibile",-1,1),new r("usione",-1,4),new r("azione",-1,2),new r("uzione",-1,4),new r("atore",-1,2),new r("ose",-1,1),new r("ante",-1,1),new r("mente",-1,1),new r("amente",19,7),new r("iste",-1,1),new r("ive",-1,9),new r("anze",-1,1),new r("enze",-1,5),new r("ici",-1,1),new r("atrici",25,1),new r("ichi",-1,1),new r("abili",-1,1),new r("ibili",-1,1),new r("ismi",-1,1),new r("usioni",-1,4),new r("azioni",-1,2),new r("uzioni",-1,4),new r("atori",-1,2),new r("osi",-1,1),new r("anti",-1,1),new r("amenti",-1,6),new r("imenti",-1,6),new r("isti",-1,1),new r("ivi",-1,9),new r("ico",-1,1),new r("ismo",-1,1),new r("oso",-1,1),new r("amento",-1,6),new r("imento",-1,6),new r("ivo",-1,9),new r("ità",-1,8),new r("istà",-1,1),new r("istè",-1,1),new r("istì",-1,1)],W=[new r("isca",-1,1),new r("enda",-1,1),new r("ata",-1,1),new r("ita",-1,1),new r("uta",-1,1),new r("ava",-1,1),new r("eva",-1,1),new r("iva",-1,1),new r("erebbe",-1,1),new r("irebbe",-1,1),new r("isce",-1,1),new r("ende",-1,1),new r("are",-1,1),new r("ere",-1,1),new r("ire",-1,1),new r("asse",-1,1),new r("ate",-1,1),new r("avate",16,1),new r("evate",16,1),new r("ivate",16,1),new r("ete",-1,1),new r("erete",20,1),new r("irete",20,1),new r("ite",-1,1),new r("ereste",-1,1),new r("ireste",-1,1),new r("ute",-1,1),new r("erai",-1,1),new r("irai",-1,1),new r("isci",-1,1),new r("endi",-1,1),new r("erei",-1,1),new r("irei",-1,1),new r("assi",-1,1),new r("ati",-1,1),new r("iti",-1,1),new r("eresti",-1,1),new r("iresti",-1,1),new r("uti",-1,1),new r("avi",-1,1),new r("evi",-1,1),new r("ivi",-1,1),new r("isco",-1,1),new r("ando",-1,1),new r("endo",-1,1),new r("Yamo",-1,1),new r("iamo",-1,1),new r("avamo",-1,1),new r("evamo",-1,1),new r("ivamo",-1,1),new r("eremo",-1,1),new r("iremo",-1,1),new r("assimo",-1,1),new r("ammo",-1,1),new r("emmo",-1,1),new r("eremmo",54,1),new r("iremmo",54,1),new r("immo",-1,1),new r("ano",-1,1),new r("iscano",58,1),new r("avano",58,1),new r("evano",58,1),new r("ivano",58,1),new r("eranno",-1,1),new r("iranno",-1,1),new r("ono",-1,1),new r("iscono",65,1),new r("arono",65,1),new r("erono",65,1),new r("irono",65,1),new r("erebbero",-1,1),new r("irebbero",-1,1),new r("assero",-1,1),new r("essero",-1,1),new r("issero",-1,1),new r("ato",-1,1),new r("ito",-1,1),new r("uto",-1,1),new r("avo",-1,1),new r("evo",-1,1),new r("ivo",-1,1),new r("ar",-1,1),new r("ir",-1,1),new r("erà",-1,1),new r("irà",-1,1),new r("erò",-1,1),new r("irò",-1,1)],L=[17,65,16,0,0,0,0,0,0,0,0,0,0,0,0,128,128,8,2,1],y=[17,65,0,0,0,0,0,0,0,0,0,0,0,0,0,128,128,8,2],U=[17],x=new n;this.setCurrent=function(e){x.setCurrent(e)},this.getCurrent=function(){return x.getCurrent()},this.stem=function(){var e=x.cursor;return i(),x.cursor=e,u(),x.limit_backward=e,x.cursor=x.limit,f(),x.cursor=x.limit,v()||(x.cursor=x.limit,b()),x.cursor=x.limit,_(),x.cursor=x.limit_backward,c(),!0}};return function(e){return"function"==typeof e.update?e.update(function(e){return i.setCurrent(e),i.stem(),i.getCurrent()}):(i.setCurrent(e),i.stem(),i.getCurrent())}}(),e.Pipeline.registerFunction(e.it.stemmer,"stemmer-it"),e.it.stopWordFilter=e.generateStopWordFilter("a abbia abbiamo abbiano abbiate ad agl agli ai al all alla alle allo anche avemmo avendo avesse avessero avessi avessimo aveste avesti avete aveva avevamo avevano avevate avevi avevo avrai avranno avrebbe avrebbero avrei avremmo avremo avreste avresti avrete avrà avrò avuta avute avuti avuto c che chi ci coi col come con contro cui da dagl dagli dai dal dall dalla dalle dallo degl degli dei del dell della delle dello di dov dove e ebbe ebbero ebbi ed era erano eravamo eravate eri ero essendo faccia facciamo facciano facciate faccio facemmo facendo facesse facessero facessi facessimo faceste facesti faceva facevamo facevano facevate facevi facevo fai fanno farai faranno farebbe farebbero farei faremmo faremo fareste faresti farete farà farò fece fecero feci fosse fossero fossi fossimo foste fosti fu fui fummo furono gli ha hai hanno ho i il in io l la le lei li lo loro lui ma mi mia mie miei mio ne negl negli nei nel nell nella nelle nello noi non nostra nostre nostri nostro o per perché più quale quanta quante quanti quanto quella quelle quelli quello questa queste questi questo sarai saranno sarebbe sarebbero sarei saremmo saremo sareste saresti sarete sarà sarò se sei si sia siamo siano siate siete sono sta stai stando stanno starai staranno starebbe starebbero starei staremmo staremo stareste staresti starete starà starò stava stavamo stavano stavate stavi stavo stemmo stesse stessero stessi stessimo steste stesti stette stettero stetti stia stiamo stiano stiate sto su sua sue sugl sugli sui sul sull sulla sulle sullo suo suoi ti tra tu tua tue tuo tuoi tutti tutto un una uno vi voi vostra vostre vostri vostro è".split(" ")),e.Pipeline.registerFunction(e.it.stopWordFilter,"stopWordFilter-it")}});