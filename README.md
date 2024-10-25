Dectector de Novedades
I need to create a function that enables me to fit a predictive model for the consumptions of each "IdProceso" from a dataframe of this form:
```
IdConsumo  IdProceso  IdGrupo  IdFecha  IdDiaSemana  IdAtipico   ConsumoMIPS
0         752999          1        1      289            3          0  0.000000e+00
1         753000       2309        3      289            3          0  2.503750e-07
2         753001          4        1      289            3         -2  1.884072e-05
3         753002          6        1      289            3         -2  1.377063e-06
4         753003          7        1      289            3         -2  1.439656e-06
```
The model must use PROPHET from Meta and here is the task the function must perform:

1.  It must identify the unique "IdProceso" of the dataframe.
2. Then it must fetch all the consumptions from the public."ConsumoMIPS" table that are labeled by -1, 0, and 1 in the "IdAtipico" column for each "IdProceso".
3. For each set of data fetched the function must fit the predictive model using PROPHET for the next days of the current month and the next month. Please, find the way to add the holydays for the corresponding lapse of time based on the Colombian calendar.
4. The predictions must be stored in a dataframe following the next squema:
```
IdPrediccion  IdProceso  IdGrupo  IdFecha  IdDiaSemana  Prediccion (y_hat)   Lim. Inf. IC (y_lower_hat)   Lim. Sup. IC (y_upper_hat)
```
5. Finally, the function must insert the data in the public."PrediccionesMIPS" table. Beaware the columns of the table are: "IdPrediccion"  "IdProceso"  "IdGrupo"  "IdFecha"  "IdDiaSemana"  "Prediccion" "LimInf" "LimSup". So the ""IdPrediccion" is the primary key and it must follow a sequential order from 1 to _; the "IdProceso" is the corresponding "IdProceso" from the dataframe; the "IdGrupo" is the corresponding "IdGrupo" from the dataframe; the "IdFecha" is the corresponding "IdFecha" from the date of the prediction, this date can be found in the public."Fechas" table in the column "IdFecha"; the "IdDiaSemana" must be added using the function add_day_of_week_id() from database/dataframe_utils.py; 