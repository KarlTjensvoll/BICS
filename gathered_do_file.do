clear all
// Change this to the directory of the datasets
cd "/Users/Karl/Dropbox/Sarah og Karl/Maastricht/Business Intelligence"

//Programs used:

capture program drop numeric_date
program numeric_date
	 tostring `1', replace format(%20.0f)
	 g temp = date(`1', "`2'" )
	 drop `1'
	 rename temp `1'
	 format `1' %d
end

capture program drop destring_date
program destring_date
	 g temp = date(`1', "`2'" )
	 drop `1'
	 rename temp `1'
	 format `1' %d
end

capture program drop destr_val
program destr_val
	replace `1' = subinstr(`1', ",", ".", .)
	destring `1', replace
end

capture program drop standardiser
program standardiser  
	qui summ `1'
	capture drop std_`1'
	g std_`1' = (`1'-`r(mean)')/`r(sd)'
end

global product1 H7493926248350
global product2 H7493926238220
// Change this to where you want the datasets to be used by Python stored
global python "/Users/Karl/Documents/#5_jupyter_notebooks/04_BICS"

// Make inventory

clear
// The inventory detail sheet needs to be its own csv file
import delimited inventory.csv, delimiters(";")
*compress
*save inventory.dta

*use inventory.dta

keep if countrycode=="AT" | ///
		countrycode=="BE" | ///
		countrycode=="BG" | ///
		countrycode=="CH" | ///
		countrycode=="CZ" | ///
		countrycode=="DE" | ///
		countrycode=="DK" | ///
		countrycode=="ES" | ///
		countrycode=="FI" | ///
		countrycode=="FR" | ///
		countrycode=="GB" | ///
		countrycode=="HU" | ///
		countrycode=="IE" | ///
		countrycode=="IT" | ///
		countrycode=="LT" | ///
		countrycode=="LV" | ///
		countrycode=="MK" | ///
		countrycode=="NL" | ///
		countrycode=="NO" | ///
		countrycode=="PL" | ///
		countrycode=="PT" | ///
		countrycode=="RS" | ///
		countrycode=="SE" 
		
assert l6=="$product1" | l6=="$product2" 
destring unrestricted_dollars, replace
destring qi_dollars, replace
destring blocked_dollars, replace

destr_val tot_inv_dollars
destr_val remaining_life

destring_date exp_date DMY

rename reportingyear year
rename ïreportingmonth month

destring customer, replace

* Drop empty columns
drop delivery lineitem marked_inhouse transport_order storage_bin ///
	sendingplant pgi_date unrestrict* qi_* blocked* restricted*
replace pp_createdon = "" if pp_createdon=="NULL"


// Here we count inventory of each hospital each month
bysort customer year month: egen tot_inv = sum(tot_inv_qty)

compress
save inventory_clean.dta, replace

/*** Generate Panel v2 ***/
drop plant salesorg type pp_create aged

capture drop inv_prod1
bysort customer year month: egen inv_prod1 = sum(tot_inv_qty) if l6=="$product1"
capture drop inv_prod2
bysort customer year month:  egen inv_prod2 = sum(tot_inv_qty) if l6=="$product2"

collapse (firstnm) countrycode stocloc stortype legalstatus ///
	 where tot_inv_dollars exp_date tot_inv inv_prod2 inv_prod1 ///
	 (mean) mean_life=remaining_life (max) max_life=remaining_life ///
	 (min) min_life=remaining_life ///
	 , by(customer year month)
replace inv_prod1 = 0 if missing(inv_prod1)
replace inv_prod2 = 0 if missing(inv_prod2)

compress
save inventory_for_panel_v2.dta, replace


// HERE WE MAKE SHIPMENT

clear

//set excelxlsxlargefile off
//import excel using BI_shipments.xlsx, firstrow

// The shipment data needs to be its own csv file
import delimited BI_shipments.csv, delimiters(";")
*import delimited shipments_v2.csv, delimiters(";")
*compress
*save shipments_v2.dta, replace
use shipments.dta
*br

// Here we take only Europe
confirm existence subregion=="Europe"
keep if subregion=="Europe"
assert subregion=="Europe"
assert region=="EMEA"
drop region subregion
drop if countryname=="Israel"
keep if upn=="$product1" | upn=="$product2"
drop movement

rename ïyear year

/*** Clean the data ***/
* Destring value with proper delimiter
destr_val value

*Destring sold/ship-tocust
destring sold*, replace
destring ship*, replace
rename ship* customer

*Create proper date
replace pgidate = substr(pgidate, 1, 10)
destring_date pgidate DMY

*br if delivdoc==848152268 & batch==20053713

*bysort customer: egen cust_max_year = max(year)
*bysort customer: egen cust_max_month = max(month)

*bysort soldtocust: egen sold_max_year = max(year)
*bysort soldtocust: egen sold_max_month = max(month)

compress
save shipments_clean.dta, replace


/*** Here we make shipments v2 to merge on hospital panel ***/
*preserve
g shipment = from=="DC" & to=="Cons"
g sale = (from=="DC" & to=="Cust") // | (from=="Cons" & to=="Cust" )

drop lvl5 l5desc upn_desc fromd salestype from_shipment_source_id to_shipment_source_id ///
	plant plant_ctry cluster hub subhub pgidate
	
keep if shipment | sale

bysort soldtocust year month: egen qty_prod1 = sum(qty) if upn=="$product1"
bysort soldtocust year month:  egen qty_prod2 = sum(qty) if upn=="$product2"

bysort soldtocust year month: egen tot_qty = sum(qty)
bysort soldtocust year month: egen tot_value = sum(value)

bysort soldtocust year month: egen numb_ship1 = count(qty) if shipment & upn=="$product1"
bysort soldtocust year month: egen numb_ship2 = count(qty) if shipment & upn=="$product2"
bysort soldtocust year month: egen numb_sale1 = count(qty) if sale & upn=="$product1"
bysort soldtocust year month: egen numb_sale2 = count(qty) if sale & upn=="$product2"


collapse (firstnm) countrycode countryname  ///
	from to (max) shipment sale (firstnm)  ///
	tot_qty tot_value qty_prod1 numb_ship1 numb_sale1 qty_prod2 numb_ship2 numb_sale2 ///
	, by(soldtocust year month)

foreach var of varlist qty_prod1 numb_ship1 numb_sale1 qty_prod2 numb_ship2 numb_sale2 {
	replace `var' = 0 if missing(`var')
}
	
compress
save shipment_soldtocust_panel_v2.dta, replace
restore

/* Here we make a panel for returns */

preserve
g returns = to=="DC"

drop lvl5 l5desc upn_desc fromd salestype from_shipment_source_id to_shipment_source_id ///
	plant plant_ctry cluster hub subhub pgidate
keep if returns

bysort soldtocust year month: egen return_qty1 = sum(qty) if upn=="$product1"
bysort soldtocust year month: egen return_qty2 = sum(qty) if upn=="$product2"

bysort soldtocust year month: egen return_tot_qty = sum(qty)
bysort soldtocust year month: egen return_tot_value = sum(value)

bysort soldtocust year month: egen numb_returns1 = count(qty) if upn=="$product1"
bysort soldtocust year month: egen numb_returns2 = count(qty) if upn=="$product2"

collapse (firstnm) from to returns return_qty1 return_qty2 ///
	return_tot_qty return_tot_value numb_returns1 numb_returns2 ///
	, by(soldtocust year month)

compress
save returns_panel_v1.dta, replace
restore

/* Here we try to capture from cons to cust */
preserve
g sale_from_cons = from=="Cons" & to=="Cust"

drop lvl5 l5desc upn_desc fromd salestype from_shipment_source_id to_shipment_source_id ///
	plant plant_ctry cluster hub subhub pgidate
	
keep if sale_from_cons

bysort soldtocust year month: egen fcons_qty_prod1 = sum(qty) if upn=="$product1"
bysort soldtocust year month: egen fcons_qty_prod2 = sum(qty) if upn=="$product2"

bysort soldtocust year month: egen fcons_tot_qty = sum(qty)
bysort soldtocust year month: egen fcons_tot_value = sum(value)

bysort soldtocust year month: egen numb_fcons1 = count(qty) if upn=="$product1"
bysort soldtocust year month: egen numb_fcons2 = count(qty) if upn=="$product2"

collapse (firstnm) from to sale_from_cons fcons_qty_prod1 fcons_qty_prod2 ///
	fcons_tot_qty fcons_tot_value numb_fcons1 numb_fcons2 ///
	, by(soldtocust year month)
	
compress
save shipment_fcons.dta, replace
restore	


// HERE WE MERGE THE SHIPMENTS AND INVENTORY

clear
use inventory_for_panel_v2.dta
rename customer soldtocust

g time_var = ym(year, month)
format %tm time_var

tsset soldtocust time_var
tsfill

gen date = dofm(time_var)
replace month = month(date) if month==.
replace year = year(date) if year==.

* Merge on shipments from DC
capture drop _merge
merge 1:1 soldtocust year month using shipment_soldtocust_panel_v2.dta ///
	, keep(master match)
	
* Merge on shipments to DC
capture drop _merge
merge 1:1 soldtocust year month using returns_panel_v1.dta ///
	, keep(master match)

* Merge on shipments from Cons to Cust
capture drop _merge
merge 1:1 soldtocust year month using shipment_fcons.dta ///
	, keep(master match)
drop exp_date countryname from to _merge

* Shipments fix missing
foreach var of varlist qty_prod1 numb_ship1 numb_sale1 qty_prod2 numb_ship2 ///
	shipment sale tot_qty tot_value numb_sale2 {
	replace `var' = 0 if missing(`var')
}

* Inventory fix missing after tsfill
foreach var of varlist tot_inv_dollars tot_inv inv_prod2 inv_prod1 ///
	mean_life max_life min_life {
	replace `var' = 0 if missing(`var')
}

* Returns fix missing
foreach var of varlist returns return_qty1 return_qty2 return_tot_qty ///
	return_tot_value numb_returns1 numb_returns2 {
	replace `var' = 0 if missing(`var')
}

* Fix missing after tsfill for string
foreach var of varlist countrycode stocloc stortype legalstatus where {
	replace `var' = `var'[_n-1] if missing(`var')
}

* Fix missing for fcons
foreach var of varlist sale_from_cons fcons_qty_prod1 fcons_qty_prod2 ///
	fcons_tot_qty fcons_tot_value numb_fcons1 numb_fcons2 {
	replace `var' = 0 if missing(`var')
}

g order_prod = shipment | sale
tsspell, cond(order == 0) seq(time_since_order)
drop _spell _end

by soldtocust: egen sum_order_prod = sum(order_prod)
by soldtocust: replace time_since_order = 99 if sum_order_prod==0

by soldtocust: egen mean_time = mean(time_since_order)
*by soldtocust: egen median_time = median(time_since_order)

compress
save full_panel_v1, replace
export delimited using full_panel_v1.csv, replace
g numb_of_orders = numb_ship1+numb_ship2+numb_sale1+numb_sale2

/* Create with lagged variables */
foreach var of varlist year month soldtocust date {
	tostring `var', replace
}

ds, has(type numeric)
di `r(varlist)'
sort soldtocust time_var
foreach var of varlist `r(varlist)' {
	 gen l_`var' = l.`var'
}

foreach var of varlist year month soldtocust date {
	destring `var', replace
}

tab countrycode, gen(dmy_)

preserve
keep soldtocust year month ///
	tot_inv order_prod tot_qty returns numb_of_orders ///
	l_tot_inv_dollars l_tot_inv l_inv_prod2 l_inv_prod1 ///
	l_mean_life l_max_life l_min_life l_shipment l_sale l_tot_qty ///
	l_tot_value l_qty_prod1 l_numb_ship1 l_numb_sale1 l_qty_prod2 ///
	l_numb_ship2 l_numb_sale2 l_returns l_return_qty1 l_return_qty2 ///
	l_return_tot_qty l_return_tot_value l_numb_returns1 l_numb_returns2 ///
	l_sale_from_cons l_fcons_qty_prod1 l_fcons_qty_prod2 l_fcons_tot_qty ///
	l_fcons_tot_value l_numb_fcons1 l_numb_fcons2 l_order_prod ///
	l_time_since_order l_mean_time l_numb_of_orders ///
	dmy_1 dmy_2 dmy_3 dmy_4 dmy_5 dmy_6 dmy_7 dmy_8 dmy_9 dmy_10 dmy_11 ///
	dmy_12 dmy_13
	
drop if l_tot_inv==.
export delimited "${python}/for_python_panel_v1.csv", replace
restore

export delimited "lagged_panel_with_all_v1.csv", replace
save lagged_panel_with_all_v1.dta, replace


/* Create a customer panel to do apriori analysis */
collapse (mean) tot_inv tot_inv_dollars inv_prod1 inv_prod2 mean_life ///
		 max_life min_life shipment sale returns sale_from_cons ///
		 time_since_order ///
		 (sum) tot_qty tot_value ///
		 qty_prod1 numb_ship1 numb_sale1 ///
		 qty_prod2 numb_ship2 numb_sale2 ///
		 return_tot_qty return_tot_value ///
		 return_qty1 numb_returns1 return_qty2 numb_returns2 ///
		 fcons_tot_qty fcons_tot_value ///
		 fcons_qty_prod1 numb_fcons1 fcons_qty_prod2 numb_fcons2 ///
		 (firstnm) countrycode stocloc stortype where ///
		 dmy_1 dmy_2 dmy_3 dmy_4 dmy_5 dmy_6 dmy_7 dmy_8 dmy_9 dmy_10 dmy_11 ///
		 dmy_12 dmy_13 ///
		 , by(soldtocust)

* Histograms to make variables
/*
hist tot_inv, bin(100) xlabel(0(1)30)
hist tot_inv_dollars, bin(200) xlabel(0(100)1500)
hist tot_qty if tot_qty<70 & tot_qty>0, bin(100) xlabel(0(5)80)
hist return_tot_qty, bin(20)
hist shipment
hist mean_life, bin(100) xlabel(-350(50)500)
hist min_life, bin(100) xlabel(-400(50)500)
hist tot_value */


capture drop shelf_life
g shelf_life = "One year or more"
	replace shelf_life = "Less than one year" if min_life<365
	replace shelf_life = "Less than half a year" if min_life<182.5
	replace shelf_life = "Expired" if min_life<=0
tab shelf_life, mi

capture drop freq_order // THIS IS WRONG!
g freq_order = "Order every 6. month or less"
	replace freq_order = "Order between every 6. and 2. month" if shipment>0.5
	replace freq_order = "Order every 2. month or more" if shipment>10/12
	replace freq_order = "Freq NA" if shipment==0
tab freq_order, mi

capture drop order_mixture
g order_mixture = "Order both products"
	replace order_mixture = "Order only prod1" if qty_prod2==0
	replace order_mixture = "Order only prod2" if qty_prod1==0
	replace order_mixture = "Mixture NA" if qty_prod1==0 & qty_prod2==0
tab order_mixture, mi

capture drop returns
g returns = "Return" if return_tot_qty>=1
	replace returns = "No return" if return_tot_qty==0
tab returns, mi
	
capture drop sum_qty
g sum_qty = "Order many prod"
	replace sum_qty = "Order moderate prod" if tot_qty<15
	replace sum_qty = "Order few prod" if tot_qty<=5
	replace sum_qty = "Order no prod" if tot_qty==0
tab sum_qty, mi

capture drop mean_inv_value
g mean_inv_value = "Inv>=200$"
	replace mean_inv_value = "Inv=150$<200$" if tot_inv_dollars<200
	replace mean_inv_value = "Inv=100$<150$" if tot_inv_dollars<150
	replace mean_inv_value = "Inv<100$" if tot_inv_dollars<100
tab mean_inv_value, mi

capture drop mean_inv
g mean_inv = "1 inventory or less" if tot_inv<=1
	replace mean_inv = "2 invenotry or less, but more than 1" ///
		if tot_inv>1 & tot_inv<=2
	replace mean_inv = "More than 2 inventory" if tot_inv>2
tab mean_inv, mi

export delimited collapsed_panel_v1.csv, replace

preserve
ds, has(type numeric)
keep `r(varlist)'
drop shipment sale returns sale_from_cons
compress
export delimited "${python}/crosssectional_PCA_cluster.csv" ///
	, replace
	
restore

keep countrycode stocloc stortype where shelf_life freq_order order_mixture ///
	return_qty sum_qty mean_value mean_inv
compress
export delimited "${python}/crosssectional_apriori.csv", replace
