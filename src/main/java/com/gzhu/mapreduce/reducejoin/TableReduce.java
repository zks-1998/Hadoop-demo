package com.gzhu.mapreduce.reducejoin;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReduce extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> tableBeans = new ArrayList<>();

        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if("order".equals(value.getFlag())){
                // 说明是 01 1001 1 order这样的行数据

                TableBean tableBean = new TableBean();

                try {
                    BeanUtils.copyProperties(tableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                tableBeans.add(tableBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean bean : tableBeans) {
            bean.setPname(pdBean.getPname());

            context.write(bean,NullWritable.get());
        }
    }
}
