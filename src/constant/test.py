# Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale
# Price;Date Open Ended Schemes ( Equity Scheme - Large Cap Fund ) Axis Mutual Fund 120466;Axis Bluechip Fund -
# Direct Plan - Dividend;INF846K01DN3;INF846K01DO1;17.23;;;01-Jan-2019

a = "Open Ended Schemes ( Equity Scheme - Large Cap Fund )"
index = a.index( '(' )
print( 'First ( index:', index )
schemtype = a[0:index - 1]
print( 'schemtype:', schemtype )

index_of_scd = a.index( '-' )
print( 'hipen:', index_of_scd )

sc_fnd_type = a[index + 2:index_of_scd]
print( 'sc_fnd_type:', sc_fnd_type )

index_lst_bck = a.index( ')' )
print( 'index_lst_bck:', index_lst_bck )
sch_fnd_siz_type = a[index_of_scd + 2:index_lst_bck - 1]
print( 'sch_fnd_siz_type:', sch_fnd_siz_type )

value_line = "135120;Axis Equity Saver Fund - Direct Plan - Growth;INF846K01VJ3;;12.93;;;01-Jan-2019"
ech_vlu_ar = value_line.split( ';' )
print( ech_vlu_ar )
sch_code = int( ech_vlu_ar[0] )
print( 'sch_code:', sch_code )

header = "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase " \
         "Price;Sale Price;Date "

header_arr = header.split( ';' )
print( 'header_arr:', header_arr )
index_sc_code = header_arr.index( 'Scheme Code' )
print( 'index_sc_code:', index_sc_code )

sch_code_value = ech_vlu_ar[index_sc_code]
print( 'sch_code_value:', sch_code_value )

index_sc_name = header_arr.index( 'Scheme Name' )
print( 'index_sc_name', index_sc_name )
sc_name_value = ech_vlu_ar[index_sc_name]
print( 'sc_name_value:', sc_name_value )
