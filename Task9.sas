/* 
21.01.2023
Решение задачи 9: создание макроса для генерации формата на основе справочника.
*/

/* 
Описание макроса: 

На вход макрос получает:,
1. таблицу фактов (mvInputTable), из которой требуется выделить измерение.
2. таблицу справочника (mvDictTable).
3. название поля для обновления справочника (mvVar) и замены его в таблице фактов.
4. название ключевого поля измерения (mvId), по которому производить связь таблиц mvInputTable и mvDictTabl.
5. название формата (mvFormatName).
6. каталог для хранения формата (mvFmtCat).

На выходе макроса: таблица (mvOutputTable), в которой столбцы измерения (mvVar) заменены на ключ измерения mvIDVar. Ключ измерения выводится в формате mvFormatName. 

Описание работы макроса:
1. Проверка наличия таблицы mvDimensionTable, если она отсутствует, то создается.
2. Проверка наличия поля mvVar в таблице фактов.
3. Проверка наличия поля mvId в mvDimensionTable и создание поля, если оно отсутствует.
4. Производится выделение значений для измерения из таблицы фактов mvInputTable, выполняться сверка, есть ли уже такие наборы значений в измерении.
5. Если какие-то наборы данных отстутствуют в таблице измерений, то они добавляются с ID=max(ID)+1.
6. Формируется выходная таблица удалением поля, перешедшего в измерение (вместо них остается ключ измерения в формате mvFormatName).
*/

proc datasets nolist nodetails lib=work kill;

%macro mFormatCreation(mvOutputTable, mvInputTable, mvDictTable, mvVar, mvId, mvFormatName, mvFmtCat);

	%local mvDataSetID mvVarEx mvCloseResult mvMaxId;

		/* Проверка наличия таблицы mvDimensionTable */

	%let mvDataSetID=%sysfunc(exist(&mvDictTable.));
	%if(&mvDataSetID. = 0) %then %do;
	
		proc sort data=&mvInputTable. out=&mvDictTable. (keep=&mvVar.) nodupkey;
		  by &mvVar.;
		run;
		
		data &mvDictTable.;
			set &mvDictTable.;
			&mvId. = _N_;
		run;
		
	%end; %else %do;
		
			/* Проверка наличия поля mvVar в таблице фактов */
	  
		%let mvVarEx=0;
	  
		proc contents DATA = &mvInputTable. out = tempContentsInpt(keep = NAME); 
		run;
	  
	  	data _NULL_;
			set tempContentsInpt (where=( lower(NAME) = lower("&mvVar.")));
			call symputx("mvVarEx", 1);
	  	run;

		%if &mvVarEx. = 0 %then %do;
			%put ERROR: Поля &mvVar. нет!;
			%abort;
		%end;	
	  
		/* 	Проверка наличия поля mvId в mvDimensionTable */
		
		%let mvDataSetID = %sysfunc(open(&mvDictTable., i));

		%if %sysfunc(varnum(&mvDataSetID., &mvId.)) eq 0 %then %do;
			%let mvCloseResult=%sysfunc(close(&mvDataSetID.));
			data &mvDictTable.;
				set &mvDictTable.;
				&mvId. = _N_;
			run;
		%end;

		%let mvCloseResult=%sysfunc(close(&mvDataSetID.));

		proc sql noprint;

			create table mfrmtcr_tmp_diff_table as
			select 
				&mvVar. 
			from &mvInputTable.
			except
			select 
				&mvVar. 
			from &mvDictTable.; 

			select 
				max(&mvId.) 
					into :mvMaxId trimmed
			from &mvDictTable.;
		
		quit;

		/* Добавляем в mvDictTable новые значения mvVar с новыми mvId , при помощи объединения чередованием.
		 Объединение простое в данном случае не срабатывает.*/
			
		data &mvDictTable.(drop = tmp);
			retain tmp &mvMaxId.;
				set &mvDictTable. mfrmtcr_tmp_diff_table;
				by &mvVar.;
			if &mvId. = . then do; 
				tmp + 1;
				&mvId. = tmp;
			end;
		run;
		
	%end;

		/* Создаем  mvFormatName на основе mvDictTable */
	
	data mfrmtcr_tmp_table_format;
		keep Start Label FmtName;
		retain FmtName "&mvFormatName.";
		set &mvDictTable. (rename=(&mvId.=Start &mvVar.=Label));
	run;

	proc sort data=mfrmtcr_tmp_table_format;
		by Start;
	run;

	proc format library=&mvFmtCat. cntlin=mfrmtcr_tmp_table_format;
	run; 
		
	proc sort data = &mvInputTable.;
		by &mvVar.;
	run;
		
	proc sort data = &mvDictTable.;
		by &mvVar.;
	run;

	data &mvOutputTable.(drop = &mvVar.);
		merge
			&mvDictTable.
			&mvInputTable.
		;
		by &mvVar.;
		format &mvId. &mvFormatName..;
	run;
		
	proc datasets lib=work noprint;
		delete mfrmtcr_tmp_table_format mfrmtcr_tmp_diff_table;
	run;
		
	%put NOTE: Временные таблицы удалены;

%mend mFormatCreation;

data src;
  set sashelp.cars ;
  if _N_ <= 10;
run;

%mFormatCreation(OutputTable, src, dm, Make, IDVar, FormatName, work);

proc print data=dm;
run;

data src;
  set sashelp.cars;
run;

%mFormatCreation(OutputTable, src, dm, Make, IDVar, FormatName, work);

proc print data=dm;
run;

data noformat;
  set OutputTable;
  format _all_;
run;
