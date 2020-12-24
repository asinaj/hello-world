OPTIONS VALIDVARNAME=any;
libname datafile "C:\Users\asinaj\Desktop\ENEL\LAST DATA";
libname output "C:\Users\asinaj\Desktop\ENEL\OUTPUT";

data DM_MENSILE_CGTE (keep=
	anno_mese
	entita
	anno
	mese
	primo_gg_mese
	ore_mese
	ore_indisp_gruppo
	ore_dispon_gruppo
	ore_indisp_centrale
	ore_dispon_centrale
	ore_indisp_amis
	ore_disp_amis
	ore_indisp_amis_gruppo
	ore_dispo_amis_gruppo
	ore_indisp_cause_esterne
	num_blocchi_amis
	kdag_numeratore
	kg_consumo_soda
	num_fs_gruppo
	num_fs_amis
	plant
	);
	set datafile.ZECOGEOSAS01;
	length 
		anno_mese $11. 
		entita $150. ;
	format 
		anno_mese $7. 
		entita $100.
		anno 11.
		mese 4.  
		primo_gg_mese DATE10.
		ore_mese 6. 
		ore_indisp_gruppo 12.2
		ore_dispon_gruppo 12.2
		ore_indisp_centrale 12.2 
		ore_dispon_centrale 12.2 
		ore_indisp_amis 12.2 
		ore_disp_amis 12.2
		ore_indisp_amis_gruppo 12.2 
		ore_dispo_amis_gruppo 12.2
		ore_indisp_cause_esterne 12.2
		num_blocchi_amis 20.
		num_fs_gruppo 20. 
		num_fs_amis 20.
	;
	label 
		anno_mese = anoo_mese
		entita = entita
		anno = anno
		mese = mese
		primo_gg_mese = primo_gg_mese
		ore_mese = ore_mese
		ore_indisp_gruppo = ore_indisp_gruppo
		ore_dispon_gruppo = ore_dispon_gruppo
		ore_indisp_centrale = ore_indisp_centrale
		ore_dispon_centrale = ore_dispon_centrale
		ore_indisp_amis = ore_indisp_amis
		ore_disp_amis = ore_disp_amis
		ore_indisp_amis_gruppo = ore_indisp_amis_gruppo
		ore_dispo_amis_gruppo = ore_dispo_amis_gruppo
		ore_indisp_cause_esterne = ore_indisp_cause_esterne
		num_blocchi_amis = num_blocchi_amis
		kdag_numeratore = kdag_numeratore
		kg_consumo_soda = kg_consumo_soda
		num_fs_gruppo = num_fs_gruppo
		num_fs_amis = num_fs_amis
	;
   anno_mese =cats('0CALYEAR'n,'-','0CALMONTH2'n); 
	entita =left('4ZECOGEOSAS01_ENTITA'n);
	/*'4ZECOGEOSAS01_ENTITA'=upcase('4ZECOGEOSAS01_IMPIANTO');
	'4ZECOGEOSAS01_ENTITA'=upcase('2FZEOPGEOPLANT');*/
	anno =input('0CALYEAR'n, $12.);
	mese =input('0CALMONTH2'n, $6.);
	primo_gg_mese =input('0CALDAY'n, yymmdd8.);
	ore_mese = '4ZECOGEOSAS01_ORE_MESE'n;
	ore_indisp_gruppo ='4ZECOGEOSAS01_ORE_IND_GR'n;
	ore_dispon_gruppo ='4ZECOGEOSAS01_ORE_DISP_GR'n;
	ore_indisp_centrale ='4ZECOGEOSAS01_ORE_IND_CEN'n;
	ore_dispon_centrale ='4ZECOGEOSAS01_ORE_DISP_CEN'n;
	ore_indisp_amis ='4ZECOGEOSAS01_ORE_IND_AMIS'n;
	ore_disp_amis ='4ZECOGEOSAS01_ORE_DISP_AMI'n;
	ore_indisp_amis_gruppo ='4ZECOGEOSAS01_ORE_DISP_AMI'n;
	ore_dispo_amis_gruppo ='4ZECOGEOSAS01_ORE_DISP_A_0'n;
	ore_indisp_cause_esterne = '4ZECOGEOSAS01_ORE_IND_EXT'n;
	num_blocchi_amis = '4ZECOGEOSAS01_NUM_BL_AMIS'n;
	kdag_numeratore = '4ZECOGEOSAS01_KDAG_NUMERAT'n;
	num_fs_gruppo ='4ZECOGEOSAS01_NUM_FS_GRU'n;
	num_fs_amis ='4ZECOGEOSAS01_NUM_FS_GR_AM'n;
	plant=left('2FZEOPGEOPLANT'n);
	
run;

data zecogeokg1(keep=anno mese plant kg_consumo_soda);
set datafile.zecogeokg;
anno=input('4ZECOGEOKG_ANNO_NR'n,4.);
mese=input('4ZECOGEOKG_MESE_NR'n,4.);
plant=left('4ZECOGEOKG_PLANT'n);
kg_consumo_soda='4ZECOGEOKG_CONSUMO_SODA'n;
if kg_consumo_soda=. then delete;
run;

proc sort data=zecogeokg1 nodupkey;
by anno mese plant;
run;

proc sql;
create table DM_MENSILE_CGTE1 as 
select a.*,b.kg_consumo_soda  label='kg_consumo_soda' format  22.5 
from DM_MENSILE_CGTE as a left join zecogeokg1 as b
on a.anno=b.anno and
a.mese=b.mese and 
a.plant=b.plant
;quit;


/*NOTE:
Regarding the following varibles we haven't found a match 
with the ones of datasets (ZECOGEOKG, ZECOGEOSAS01)
ore_sfioro
fluido_sfiorato
ore_fs_maggiori_1ora
potenza_nominale_mw
potenza_media_mensile_lorda_mw
netta_metering_gwh
ore_di_funzionamento
ore_fuoriservizio_centrale
ore_sfioro_0001
t_fluido_sfiorato
tot_fluido_geo_ingresso_t
temp_media_ingresso_reattore
delta_t_media_del_reattore
varianza_delta_t
temp_max_bypass_amis_scar_emer */

/*Proc sql;
create table tot_farinello as
Select month, year, hour month, kdag, sum (hour month, - kdag) as hour_disp_kdat
From sas01 extract
Where entita in ('farinelloa', 'farinellob')
; quit;


Proc sql;
create table tot_farinello2 as
Select month, year, sum (hours_disp_kdat) as hours_disp_kdat
From tot_farinello
Group by 1,2
; quit;

 
date tot_farinello2;
set tot_farinello2;
length farinelloa farinellob $ ...;
entity = 'farinelloa'; output;
entity = 'farinellob'; output;
run;

 
proc sql;
create final table as
select a. *, b. hours_disp_kdat
from sas01estrato as a left join tot_farinello2 as b
on a.month = b.month and a.year = b.year and a.entita = b.entita
; quit;


final date;
final set;
if entity in ('farinelloa', 'farinellob') then do;
soda = soda * (hour-month-kdag) / hour_disp_kdat;
end;
run;*/