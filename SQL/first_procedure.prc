CREATE OR REPLACE PROCEDURE transe_netnp_olddata
(
 v_acceptMonth   IN  VARCHAR2,
 v_result_401        OUT VARCHAR2,
 v_result_402        OUT VARCHAR2
)
IS
  iv_count               NUMBER;
  iv_in_user_id          NUMBER(16);
  iv_out_user_id         NUMBER(16);
  iv_serial_number       VARCHAR2(20);
  iv_trade_id            NUMBER(16);
  iv_in_eparchy_code     VARCHAR2(4);
  iv_out_eparchy_code    VARCHAR(4);
  iv_out_user_tag_set    VARCHAR(50);
  iv_start_date          DATE;
  iv_trade_staffId       VARCHAR2(100);
  iv_trade_deptId        VARCHAR2(100);
  iv_in_platsvc_instid NUMBER(16);
  iv_in_attr_instid    NUMBER(16);
  iv_sysdate           DATE;

  -- 游标
  TYPE t_cursor IS REF CURSOR;
  inNpTradeInfo  t_cursor;


BEGIN

    BEGIN
      SELECT COUNT(1) INTO iv_count
        FROM tf_table T
       WHERE T.TRADE_TYPE_CODE = '401'
         AND T.ACCEPT_MONTH = V_ACCEPTMONTH
         AND T.CANCEL_TAG = '0';

       IF iv_count = 0 THEN
         v_result_401 := 'XZXCZXCZX';
         
         RETURN;
       END IF;
       
    END;
    
    iv_sysdate := SYSDATE;
    
 

    OPEN inNpTradeInfo FOR
        SELECT T.TRADE_ID, T.SERIAL_NUMBER, T.USER_ID, T.Trade_Staff_Id, T.Trade_Depart_Id
          FROM tf_table T
         WHERE T.TRADE_TYPE_CODE = '401'
           AND T.ACCEPT_MONTH = V_ACCEPTMONTH
           AND T.Cancel_Tag = '0';


    LOOP
       FETCH inNpTradeInfo
       into iv_trade_id,iv_serial_number,iv_in_user_id,iv_trade_staffId, iv_trade_deptId;
       exit when inNpTradeInfo%NOTFOUND;

       BEGIN


             -- 老数据tf_table_NETNP表RSRV_STR5为用户原来的USER_TAG_SET
             SELECT TNP.IN_EPARCHY_CODE, TNP.OUT_USER_ID, TNP.OUT_EPARCHY_CODE, TNP.RSRV_STR5, TNP.PORT_IN_DATE
               INTO iv_in_eparchy_code, iv_out_user_id, iv_out_eparchy_code, iv_out_user_tag_set, iv_start_date
               FROM tf_table_NETNP TNP
              WHERE TNP.TRADE_ID = iv_trade_id;

               -- 主台账intf_id、book_date
             UPDATE tf_table
             SET book_date = exec_time,
                 intf_id = 'tf_table_PAYRELATION,tf_table_BRANDCHANGE,tf_table_NETNP,tf_table_ACCOUNT'
                         ||',tf_table_INTEGRALACCT,tf_table_ACCOUNT_ACCTDAY,tf_table_INTEGRALPLAN,tf_table_SVC'
                         ||',tf_table_DISCNT,tf_table_CUSTOMER,tf_table_SCORERELATION,tf_table_USER'
                         ||',tf_table_SVCSTATE,tf_table_PRODUCT,tf_table_CUST_PERSON,tf_table_USER_ACCTDAY'
                         ||',tf_table,tf_table_PLATSVC,tf_table_ATTR,tf_table_POST,tf_table_ACCT_CONSIGN,'
             WHERE trade_id = iv_trade_id;


            v_result_401 := v_result_401 || '[SERIAL_NUMBER:' || iv_serial_number
                                  || ',IN_EPARCHY_CODE:' || iv_in_eparchy_code
                                  || ',IN_USER_ID:' || iv_in_user_id
                                  || ',OUT_EPARCHY_CODE:' || iv_out_eparchy_code
                                  || ',OUT_USER_ID:' || iv_out_user_id
                                  || ']';
            
             =======================================================================================================
             IF iv_out_eparchy_code = '0029' THEN
                 -- 插入平台服务台账
                  FOR platsvc IN (SELECT iv_trade_id TRADE_ID, v_acceptMonth ACCEPT_MONTH,
                                    iv_in_user_id USER_ID,
                                    '0' IS_NEED_PF, SYSDATE OPER_TIME, '0' MODIFY_TAG,
                                    B.OPER_CODE, B.OPR_SOURCE,
                                    A.PRODUCT_ID, A.PACKAGE_ID, A.SERVICE_ID,
                                    A.BIZ_STATE_CODE, A.FIRST_DATE, A.FIRST_DATE_MON,
                                    A.GIFT_SERIAL_NUMBER, A.GIFT_USER_ID, A.IN_CARD_NO,
                                    A.ENTITY_CARD_NO, A.INST_ID,
                                    A.START_DATE, A.END_DATE, A.UPDATE_TIME,
                                    A.UPDATE_STAFF_ID, A.UPDATE_DEPART_ID, A.REMARK,
                                    A.RSRV_NUM1, A.RSRV_NUM2, A.RSRV_NUM3, A.RSRV_NUM4,
                                    A.RSRV_NUM5, A.RSRV_STR1, A.RSRV_STR2, A.RSRV_STR3,
                                    A.RSRV_STR4, A.RSRV_STR5, A.RSRV_STR6, A.RSRV_STR7,
                                    A.RSRV_STR8, A.RSRV_STR9, A.RSRV_STR10, A.RSRV_DATE1,
                                    A.RSRV_DATE2, A.RSRV_DATE3, A.RSRV_TAG1, A.RSRV_TAG2,
                                    A.RSRV_TAG3
                               FROM UOP_CRM1.TF_F_USER_PLATSVC@DBLNK_NGCRMDB1 A, 
                                    UOP_CRM1.TF_F_USER_PLATSVC_TRACE@DBLNK_NGCRMDB1 B
                              WHERE A.user_id = iv_out_user_id
                                AND B.user_id = iv_out_user_id
                                AND A.BIZ_STATE_CODE <> 'E'
                                AND A.end_date > SYSDATE
                                AND B.rela_inst_id = A.inst_id)
                   LOOP


                      SELECT seq_inst_id.NEXTVAL INTO iv_in_platsvc_instid FROM dual;
                      iv_in_platsvc_instid := SUBSTR(iv_in_eparchy_code, 3, 4) || to_char(iv_sysdate, 'yyMMdd') || iv_in_platsvc_instid;

                      INSERT INTO tf_table_PLATSVC
                        (TRADE_ID, ACCEPT_MONTH, USER_ID,
                         IS_NEED_PF, OPER_TIME, MODIFY_TAG, OPER_CODE, OPR_SOURCE,
                         PRODUCT_ID, PACKAGE_ID, SERVICE_ID,
                         BIZ_STATE_CODE, FIRST_DATE, FIRST_DATE_MON, GIFT_SERIAL_NUMBER,
                         GIFT_USER_ID, INST_ID, IN_CARD_NO, ENTITY_CARD_NO, START_DATE,
                         END_DATE, UPDATE_TIME, UPDATE_STAFF_ID, UPDATE_DEPART_ID, REMARK,
                         RSRV_NUM1, RSRV_NUM2, RSRV_NUM3, RSRV_NUM4, RSRV_NUM5, RSRV_STR1,
                         RSRV_STR2, RSRV_STR3, RSRV_STR4, RSRV_STR5, RSRV_STR6, RSRV_STR7,
                         RSRV_STR8, RSRV_STR9, RSRV_STR10, RSRV_DATE1, RSRV_DATE2, RSRV_DATE3,
                         RSRV_TAG1, RSRV_TAG2, RSRV_TAG3)
                      VALUES
                        (platsvc.TRADE_ID, platsvc.ACCEPT_MONTH, platsvc.USER_ID,
                         platsvc.IS_NEED_PF, iv_sysdate, platsvc.MODIFY_TAG,
                         platsvc.OPER_CODE,platsvc.OPR_SOURCE,
                         platsvc.PRODUCT_ID, platsvc.PACKAGE_ID,
                         platsvc.SERVICE_ID, platsvc.BIZ_STATE_CODE, platsvc.FIRST_DATE,
                         platsvc.FIRST_DATE_MON, platsvc.GIFT_SERIAL_NUMBER, platsvc.GIFT_USER_ID,

                         iv_in_platsvc_instid,

                         platsvc.IN_CARD_NO, platsvc.ENTITY_CARD_NO,

                         iv_start_date,
                         platsvc.END_DATE,
                         iv_sysdate,
                         iv_trade_staffId,
                         iv_trade_deptId,
                         '携转数据处理',

                         platsvc.RSRV_NUM1, platsvc.RSRV_NUM2,
                         platsvc.RSRV_NUM3, platsvc.RSRV_NUM4, platsvc.RSRV_NUM5, platsvc.RSRV_STR1,
                         platsvc.RSRV_STR2, platsvc.RSRV_STR3, platsvc.RSRV_STR4, platsvc.RSRV_STR5,
                         platsvc.RSRV_STR6, platsvc.RSRV_STR7, platsvc.RSRV_STR8, platsvc.RSRV_STR9,
                         platsvc.RSRV_STR10, platsvc.RSRV_DATE1, platsvc.RSRV_DATE2, platsvc.RSRV_DATE3,
                         platsvc.RSRV_TAG1, platsvc.RSRV_TAG2, platsvc.RSRV_TAG3);

                         -- 属性
                         FOR platattr IN (SELECT iv_in_platsvc_instid RELA_INST_ID, iv_trade_id TRADE_ID,
                                    v_acceptMonth ACCEPT_MONTH, iv_in_user_id USER_ID, INST_TYPE,
                                    INST_ID, ATTR_CODE, ATTR_VALUE, START_DATE, END_DATE,
                                    '0' MODIFY_TAG, UPDATE_TIME, UPDATE_STAFF_ID,
                                    UPDATE_DEPART_ID, REMARK, RSRV_NUM1, RSRV_NUM2, RSRV_NUM3,
                                    RSRV_NUM4, RSRV_NUM5, RSRV_STR1, RSRV_STR2, RSRV_STR3,
                                    RSRV_STR4, RSRV_STR5, RSRV_DATE1, RSRV_DATE2, RSRV_DATE3,
                                    RSRV_TAG1, RSRV_TAG2, RSRV_TAG3, ELEMENT_ID
                               FROM UOP_CRM1.TF_F_USER_ATTR@DBLNK_NGCRMDB1
                              WHERE  user_id = iv_out_user_id AND RELA_INST_ID = platsvc.INST_ID)
                          LOOP

                              SELECT seq_inst_id.NEXTVAL INTO iv_in_attr_instid FROM dual;
                              iv_in_attr_instid := SUBSTR(iv_in_eparchy_code, 3, 4) || to_char(iv_sysdate, 'yyMMdd') || iv_in_attr_instid;
                              
                              INSERT INTO tf_table_ATTR
                                (TRADE_ID, ACCEPT_MONTH, RELA_INST_ID, USER_ID, MODIFY_TAG,
                                 INST_TYPE, INST_ID, ATTR_CODE,
                                 ATTR_VALUE, START_DATE, END_DATE, UPDATE_TIME, UPDATE_STAFF_ID,
                                 UPDATE_DEPART_ID, REMARK, RSRV_NUM1, RSRV_NUM2, RSRV_NUM3, RSRV_NUM4,
                                 RSRV_NUM5, RSRV_STR1, RSRV_STR2, RSRV_STR3, RSRV_STR4, RSRV_STR5,
                                 RSRV_DATE1, RSRV_DATE2, RSRV_DATE3, RSRV_TAG1, RSRV_TAG2, RSRV_TAG3,
                                 ELEMENT_ID)
                              VALUES
                                (platattr.TRADE_ID, platattr.ACCEPT_MONTH, platattr.RELA_INST_ID,
                                 platattr.USER_ID, platattr.MODIFY_TAG, platattr.INST_TYPE,
                                 iv_in_attr_instid,
                                 platattr.ATTR_CODE, platattr.ATTR_VALUE,

                                 iv_start_date,
                                 platattr.END_DATE,
                                 iv_sysdate,
                                 iv_trade_staffId,
                                 iv_trade_deptId,
                                 '携转数据处理',

                                 platattr.RSRV_NUM1, platattr.RSRV_NUM2,
                                 platattr.RSRV_NUM3, platattr.RSRV_NUM4, platattr.RSRV_NUM5, platattr.RSRV_STR1,
                                 platattr.RSRV_STR2, platattr.RSRV_STR3, platattr.RSRV_STR4, platattr.RSRV_STR5,
                                 platattr.RSRV_DATE1, platattr.RSRV_DATE2, platattr.RSRV_DATE3, platattr.RSRV_TAG1,
                                 platattr.RSRV_TAG2, platattr.RSRV_TAG3, platattr.ELEMENT_ID);

                           
                            END LOOP;
                         

                    END LOOP;

                 
                 COMMIT;
             
               
             END IF;
             
            --==================================================================================================================== 
             
             v_result_401 := v_result_401 || '处理成功。' ;
             
                              
              EXCEPTION
                WHEN OTHERS THEN
                v_result_401 := v_result_401 || '处理失败。' || SQLERRM; 
                ROLLBACK;  
                
                           
         
         END;

    END LOOP;
    
    
    --====================================================================================================
    
        FOR netnptrade IN (
                            SELECT SERIAL_NUMBER,OUT_USER_ID,OUT_EPARCHY_CODE,IN_EPARCHY_CODE,
                                   RSRV_STR5 OUT_USER_TAG_SET
                             FROM UOP_CRM1.tf_table_NETNP@DBLNK_NGCRMDB1
                            WHERE STATE = '0' 
                             AND   CANCEL_TAG = '0'
                             AND   MODIFY_TAG = '0'
                             AND   ACCEPT_MONTH = v_acceptmonth 
                           )
        LOOP 
          BEGIN 
              v_result_402 := v_result_402 || '[SERIAL_NUMBER:' || netnptrade.SERIAL_NUMBER
                           || ',OUT_USER_ID:' || netnptrade.OUT_USER_ID
                           || ',OUT_EPARCHY_CODE:' || netnptrade.OUT_EPARCHY_CODE
                           || ']';
              UPDATE TF_BH_TRADE 
               SET rsrv_str1 = netnptrade.IN_EPARCHY_CODE, 
                   rsrv_str2 = netnptrade.OUT_EPARCHY_CODE,
                   rsrv_str3 = netnptrade.OUT_USER_TAG_SET
              WHERE trade_type_code = '402' 
               AND user_id = netnptrade.OUT_USER_ID
               AND accept_month = v_acceptmonth;
               
              COMMIT;
              v_result_402 := v_result_402 || '处理成功！';
              EXCEPTION
                    WHEN OTHERS THEN
                    v_result_402 := v_result_402 || '处理失败！' || SQLERRM; 
                    ROLLBACK;
            END;
        END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN
      v_result_401 := v_result_401 || '处理失败。' || SQLERRM;
      v_result_402 := v_result_402 || '处理失败。' || SQLERRM;
END;
/
