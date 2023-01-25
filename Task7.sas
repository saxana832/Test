/*
18.12.2022
Решение задачи 7: создание макроса для выделения измерения из таблицы.
*/

/*
Описание макроса:

На вход макрос получает:,
1. таблицу фактов (mvInputTable), из которой требуется выделить измерение.
2. таблицу измерения (mvDimensionTable).
3. поле, которое нужно вынести в измерение (mvVar).
4. название ключевого поля измерения (mvIDVar), по которому производить связь таблиц mvInputTable и mvDimensionTable.

На выходе макроса: таблица (mvOutputTable), в которой столбец измерения (mvVar) заменен на ключ измерения mvIDVar.

Описание работы макроса:
1. Проверка наличия поля mvVar в таблице фактов (mvInputTable).
2. Проверка наличия таблицы mvDimensionTable, если она отсутствует, то создается.3. Производится выделение значений для измерения из таблицы фактов mvInputTable, выполняться сверка, есть ли уже такие наборы значений в измерении.
4. Если какие-то наборы данных отстутствуют в таблице измерений, то они добавляются с ID=max(ID)+1.
5. Формируется выходная таблица удалением поля, перешедшего в измерение (вместо него остается ключ измерения).
*/
proc datasets nolist nodetails lib= work kill;
run;

%macro mExtract1Dimension(mvoutputTable, mvInputTable, mvDimensionTable, mvVar, mvIDVar);
    %local mvDataSetID mvVarEx mvMaxId mvDebugPrint mvDebugDelete;

    %let mvDebugPrint= noprint;
    %let mvDebugDelete= 1;

    /* Проверка наличия поля mvVar в таблице фактов */
    %let mvVarEx= 0;

    proc contents data=&mvInputTable out=mextdim_temp_contents_inpt (keep=name) &mvDebugPrint;
    run;

    data _NULL_;
        set mextdim_temp_contents_inpt (where=(lower(name) = lower("&mvVar.")));
        call symputx("mvVarEx", 1);
    run;

    %if (&mvVarEx. = 0) %then %do;
        %put ERROR: Поля &mvVar. нет!;
        %abort;
    %end;


    /* Проверка наличия таблицы mvDimensionTable */
    %let mvDataSetID= %sysfunc(exist(&mvDimensionTable));
    %if (&mvDataSetID. = 0) %then %do;
        proc sort data=&mvInputTable out=&mvDimensionTable (keep=&mvVar) nodupkey;
            by &mvVar;
        run;

        data &mvDimensionTable;
            set &mvDimensionTable;
            &mvIDVar = _N_;
        run;
    %end; %else %do;

        proc sql &mvDebugPrint;
            create table mextdim_tmp_new_dim as
            select
                &mvVar
            from &mvInputTable
            EXCEPT
            select
                &mvVar
            from &mvDimensionTable;

            select
                max(&mvIDVar)
                    into :mvMaxId trimmed
            from &mvDimensionTable;
        quit;

    /* Добавляем в mvDimensionTable новые значения mvVar с новыми mvIDVar, при помощи объединения чередованием.
        Объединение простое в данном случае не срабатывает. */

        data &mvDimensionTable (drop=tmp);
            retain tmp &mvMaxId.;
            set &mvDimensionTable mextdim_tmp_new_dim;
            by &mvVar;
            if (&mvIDVar = .) then do;
                tmp + 1;
                &mvIDVar = tmp;
            end;
        run;
    %end;

    proc sort data=&mvInputTable;
        by &mvVar;
    run;

    proc sort data=&mvDimensionTable;
        by &mvVar;
    run;

    data &mvOutputTable;
        merge
            &mvDimensionTable
            &mvInputTable
        ;
        by &mvVar;
        drop &mvVar;
    run;

    %if (&mvDebugDelete.) %then %do;
        proc datasets lib=work &mvDebugPrint;
            delete mextdim_temp_contents_inpt mextdim_tmp_new_dim;
        run;
        %put NOTE: Временные таблицы удалены;
    %end;

%mend mExtract1Dimension;

data src;
  set sashelp.cars ;
  if (_N_ <= 10);
run;

%mExtract1Dimension(result_tbl, src, car_dim, Make, make_id);

proc print data=car_dim;
run;

data src;
    set sashelp.cars;
run;

%mExtract1Dimension(result_tbl, src, car_dim, Make, make_id);

proc print data=car_dim;
run;
