package com.hazelcast.jet.contrib.util;

import com.hazelcast.jet.contrib.model.CDCRecord;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * date: 2019-10-23
 * author: emindemirci
 */
public class SQLUtil {
    public static final String LOGMINER_SELECT_WITH_SCHEMA_SQL = "SELECT thread#, scn, start_scn, commit_scn,timestamp, operation_code, operation,status, SEG_TYPE_NAME ,info,seg_owner, table_name, username, sql_redo ,row_id, csf, TABLE_SPACE, SESSION_INFO, RS_ID, RBASQN, RBABLK, SEQUENCE#, TX_NAME, SEG_NAME, SEG_TYPE_NAME FROM  v$logmnr_contents  WHERE OPERATION_CODE in (1,2,3) and commit_scn>=? and ";
    public static final String START_LOGMINER_SQL = "begin \nDBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, OPTIONS =>  DBMS_LOGMNR.SKIP_CORRUPTION+DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT+DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.CONTINUOUS_MINE+DBMS_LOGMNR.COMMITTED_DATA_ONLY+dbms_logmnr.STRING_LITERALS_IN_STMT) \n; end;";
    public static final String STOP_LOGMINER_SQL = "begin \nSYS.DBMS_LOGMNR.END_LOGMNR; \nend;";
    public static final String CURRENT_DB_SCN_SQL = "select min(current_scn) CURRENT_SCN from gv$database";
    public static final String LASTSCN_STARTPOS = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where ? between FIRST_CHANGE# and NEXT_CHANGE# union select FIRST_CHANGE# from v$archived_log where ? between FIRST_CHANGE# and NEXT_CHANGE# and standby_dest='NO')";
    public static final String TABLE_WITH_COLS = "with dcc as (SELECT dcc.owner,dcc.table_name,dcc2.column_name,1 PK_COLUMN from dba_constraints dcc,dba_cons_columns dcc2 where dcc.owner=dcc2.owner and dcc.table_name=dcc2.table_name and dcc.constraint_name=dcc2.constraint_name and dcc.constraint_type='P'),duq as (select di2.TABLE_OWNER,di2.TABLE_NAME,di2.COLUMN_NAME , 1 UQ_COLUMN from dba_ind_columns di2 join dba_indexes di on di.table_owner=di2.TABLE_OWNER and di.table_name=di2.TABLE_NAME and di.uniqueness='UNIQUE' and di.owner=di2.INDEX_OWNER and di.index_name=di2.INDEX_NAME group by di2.TABLE_OWNER,di2.TABLE_NAME,di2.COLUMN_NAME) select dc.owner,dc.TABLE_NAME,dc.COLUMN_NAME,dc.NULLABLE,dc.DATA_TYPE,nvl(dc.DATA_PRECISION,dc.DATA_LENGTH) DATA_LENGTH,nvl(dc.DATA_SCALE,0) DATA_SCALE,nvl(dc.DATA_PRECISION,0) DATA_PRECISION,nvl(x.pk_column,0) pk_column,nvl(y.uq_column,0) uq_column from dba_tab_cols dc left outer join dcc x on x.owner=dc.owner and x.table_name=dc.TABLE_NAME and dc.COLUMN_NAME=x.column_name left outer join duq y on y.table_owner=dc.owner and y.table_name=dc.TABLE_NAME and y.column_name=dc.COLUMN_NAME where dC.Owner='$TABLE_OWNER$' and dc.TABLE_NAME='$TABLE_NAME$' and dc.HIDDEN_COLUMN='NO' and dc.VIRTUAL_COLUMN='NO' order by dc.TABLE_NAME,dc.COLUMN_ID";


    public static final String SCN_POSITION_FIELD = "scnposition";
    public static final String COMMIT_SCN_POSITION_FIELD = "commitscnposition";
    public static final String ROW_ID_POSITION_FIELD = "rowid";
    public static final String LOG_MINER_OFFSET_FIELD = "logminer";

    public static final String DML_ROW_SCHEMA_NAME = "DML_ROW";
    public static final String SCN_FIELD = "SCN";
    public static final String COMMIT_SCN_FIELD = "COMMIT_SCN";
    public static final String OWNER_FIELD = "OWNER";
    public static final String SEG_OWNER_FIELD = "SEG_OWNER";
    public static final String TABLE_NAME_FIELD = "TABLE_NAME";
    public static final String TIMESTAMP_FIELD = "TIMESTAMP";
    public static final String SQL_REDO_FIELD = "SQL_REDO";
    public static final String OPERATION_FIELD = "OPERATION";
    public static final String DATA_ROW_FIELD = "data";
    public static final String BEFORE_DATA_ROW_FIELD = "before";
    public static final String DATA_SCALE_FIELD = "DATA_SCALE";
    public static final String DATA_PRECISION_FIELD = "DATA_PRECISION";
    public static final String DATA_LENGTH_FIELD = "DATA_LENGTH";
    public static final String PK_COLUMN_FIELD = "PK_COLUMN";
    public static final String UQ_COLUMN_FIELD = "UQ_COLUMN";
    public static final String NULLABLE_FIELD = "NULLABLE";
    public static final String COLUMN_NAME_FIELD = "COLUMN_NAME";
    public static final String CSF_FIELD = "CSF";
    public static final String NULL_FIELD = "NULL";
    public static final String DATA_TYPE_FIELD = "DATA_TYPE";
    public static final String DOT = ".";
    public static final String COMMA = ",";
    public static final String ROW_ID_FIELD = "ROW_ID";
    public static final String SRC_CON_ID_FIELD = "SRC_CON_ID";

    public static final String NUMBER_TYPE = "NUMBER";
    public static final String LONG_TYPE = "LONG";
    public static final String STRING_TYPE = "STRING";
    public static final String TIMESTAMP_TYPE = "TIMESTAMP";
    public static final String DATE_TYPE = "DATE";
    public static final String CHAR_TYPE = "CHAR";
    public static final String FLOAT_TYPE = "FLOAT";
    public static final String TEMPORARY_TABLE = "temporary tables";

    public static final String OPERATION_INSERT = "INSERT";
    public static final String OPERATION_UPDATE = "UPDATE";
    public static final String OPERATION_DELETE = "DELETE";

    private SQLUtil() {
    }


    public static CDCRecord parseSqlCreateRecord(String sqlRedo) throws JSQLParserException {

        String sqlRedo2 = sqlRedo.replace("IS NULL", "= NULL");
        Statement stmt = CCJSqlParserUtil.parse(sqlRedo2);
        final Map<String, String> after = new LinkedHashMap<>();
        final Map<String, String> before = new LinkedHashMap<>();

        if (stmt instanceof Insert) {
            Insert insert = (Insert) stmt;

            for (Column c : insert.getColumns()) {
                after.put(cleanString(c.getColumnName()), null);
            }

            ExpressionList eList = (ExpressionList) insert.getItemsList();
            List<Expression> valueList = eList.getExpressions();
            int i = 0;
            for (String key : after.keySet()) {
                String value = cleanString(valueList.get(i).toString());
                after.put(key, value);
                i++;
            }

        } else if (stmt instanceof Update) {
            Update update = (Update) stmt;
            for (Column c : update.getColumns()) {
                after.put(cleanString(c.getColumnName()), null);
            }

            Iterator<Expression> iterator = update.getExpressions().iterator();

            for (String key : after.keySet()) {
                Object o = iterator.next();
                String value = cleanString(o.toString());
                after.put(key, value);
            }

            update.getWhere().accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(final EqualsTo expr) {
                    String col = cleanString(expr.getLeftExpression().toString());
                    String value = cleanString(expr.getRightExpression().toString());
                    before.put(col, value);

                }
            });

        } else if (stmt instanceof Delete) {
            Delete delete = (Delete) stmt;
            delete.getWhere().accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(final EqualsTo expr) {
                    String col = cleanString(expr.getLeftExpression().toString());
                    String value = cleanString(expr.getRightExpression().toString());
                    before.put(col, value);

                }
            });
        }
        return new CDCRecord(before, after);
    }

    private static String cleanString(String str) {
        if (str.startsWith("TIMESTAMP")) {
            str = str.replace("TIMESTAMP ", "");
        }
        if (str.startsWith("'") && str.endsWith("'")) {
            str = str.substring(1, str.length() - 1);
        }
        if (str.startsWith("\"") && str.endsWith("\"")) {
            str = str.substring(1, str.length() - 1);
        }
        return str.replace("IS NULL", "= NULL").trim();
    }


}
