/* 
17.01.2023
Решение задачи 8: создание макроса для выделения измерения из таблицы.
*/

/* 
Описание макроса: 

На вход макрос получает:,
1. таблицу фактов (mvInputTable), из которой требуется выделить измерение.
2. таблицу измерения (mvDimensionTable).
3. Название выходной таблицы (mvOutputTable).
4. поля, которые нужно вынести в измерение (mvVars).
5. название ключевого поля измерения (mvIDVar), по которому производить связь таблиц mvInputTable и mvDimensionTable.

На выходе макроса: таблица (mvOutputTable), в которой столбцы измерения (mvVars) заменены на ключ измерения mvIDVar.

Описание работы макроса:
1. Проверка наличия полей mvVars в таблице фактов (mvInputTable) и в таблице измерений (mvDimensionTable).
2. Проверка наличия таблицы mvDimensionTable, если она отсутствует, то создается.
3. Производится выделение значений для измерения из таблицы фактов mvInputTable, выполняться сверка, есть ли уже такие наборы значений в измерении.
4. Если какие-то наборы данных отстутствуют в таблице измерений, то они добавляются с ID=max(ID)+1.
5. Формируется выходная таблица удалением полей, перешедших в измерение (вместо них остается ключ измерения).
*/

proc datasets nolist nodetails lib=work kill;
run;

%macro mExtractDimension(mvoutputTable, mvInputTable , mvDimensionTable, mvVars, mvIDVar);

	%local mvDataSetID mvTmpVars mvCloseResult mvMaxId;
	%let mvTmpVars = %sysfunc(tranwrd(&mvVars., %str( ), %str(, )));

		/* 	Проверка наличия полей mvVars в mvDimensionTable */
		
	%let mvDataSetID = %sysfunc(open(&mvDimensionTable., i));

	%do i=1 %to %sysfunc(countw(&mvVars.));
		%if %sysfunc(varnum(&mvDataSetID.,%scan(&mvVars.,&i.))) eq 0 %then %do;
			%let mvCloseResult=%sysfunc(close(&mvDataSetID.));
			%put ERROR: Не найдена переменная %scan(&mvVars.,&i.) в измерении &mvDimensionTable.;
			%abort;
		%end;
	%end;

	%let mvCloseResult=%sysfunc(close(&mvDataSetID.));

		/* 	Проверка наличия полей mvVars в mvInputTable */
		
	%let mvDataSetID = %sysfunc(open(&mvInputTable., i));

	%do i=1 %to %sysfunc(countw(&mvVars.));
		%if %sysfunc(varnum(&mvDataSetID.,%scan(&mvVars.,&i.))) eq 0 %then %do;
			%let mvCloseResult=%sysfunc(close(&mvDataSetID.));
			%put ERROR: Не найдена переменная %scan(&mvVars.,&i.) в измерении &mvInputTable.;
			%abort;
		%end;
	%end;

	%let mvCloseResult=%sysfunc(close(&mvDataSetID.));

		/* Проверка наличия таблицы mvDimensionTable */

	%let mvDataSetID=%sysfunc(exist(&mvDimensionTable.));
	%if(&mvDataSetID. = 0) %then %do;
	
		proc sort data=&mvInputTable. out=&mvDimensionTable. (keep=&mvVars.) nodupkey;
			by &mvVars.;
		run;
		
		data &mvDimensionTable.;
			set &mvDimensionTable.;
			&mvIDVar. = _N_;
		run;
		
	%end; %else %do;
		proc sql noprint;

			create table mextdim_tmp_new_dim as
			select 
				&mvTmpVars. 
			from &mvInputTable.
			except
			select 
				&mvTmpVars. 
			from &mvDimensionTable.; 
		
			select max(&mvIDVar.) into :mvMaxId trimmed
				from &mvDimensionTable.;
		
		quit;

		/* Добавляем в mvDimensionTable новые значения mvVars с новыми mvIDVar, при помощи объединения чередованием.
		 Объединение простое в данном случае не срабатывает. */
		
		data &mvDimensionTable.(drop = tmp);
			retain tmp &mvMaxId.;
				set &mvDimensionTable. mextdim_tmp_new_dim;
				by &mvVars.;
			if (&mvIDVar. = .) then do; 
				tmp + 1;
				&mvIDVar. = tmp;
			end;
		run;
		
	%end;
		
	proc sort data = &mvInputTable.;
		by &mvVars.;
	run;
		
	proc sort data = &mvDimensionTable.;
		by &mvVars.;
	run;
				
	data &mvoutputTable.(drop = &mvVars.);
		merge
			&mvDimensionTable.
			&mvInputTable.
		;
		by &mvVars.;
	run;
		
	proc datasets lib=work noprint;
		delete mextdim_tmp_new_dim;
	run;
		
	%put NOTE: Временные таблицы удалены;
	
%mend mExtractDimension;


data src;
  set sashelp.cars;
  if _N_ <= 10;
run;

%mExtractDimension(OutputTable, src, dm, Make Origin, IDVar);

data src;
  set sashelp.cars;
run;

%mExtractDimension(OutputTable, src, dm, Make Origin, IDVar);