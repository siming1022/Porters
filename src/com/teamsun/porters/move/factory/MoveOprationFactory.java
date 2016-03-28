package com.teamsun.porters.move.factory;

import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.op.hdfs.Hdfs2HbaseOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2HdfsOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2MySqlOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2OracleOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2TeradataOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2VerticaOp;
import com.teamsun.porters.move.op.oracle.Oracle2HbaseOp;
import com.teamsun.porters.move.op.oracle.Oracle2HdfsOp;
import com.teamsun.porters.move.op.teradata.Teradata2Hbase;
import com.teamsun.porters.move.op.teradata.Teradata2Hdfs;
import com.teamsun.porters.move.util.Constants;

public class MoveOprationFactory 
{
	public static MoveOpration create(String type)
	{
		if (Constants.DATA_MOVE_TYPE_H2H.equals(type))
		{
			return new Hdfs2HdfsOp();
		}
		else if (Constants.DATA_MOVE_TYPE_H2O.equals(type))
		{
			return new Hdfs2OracleOp();
		}
		else if (Constants.DATA_MOVE_TYPE_H2T.equals(type))
		{
			return new Hdfs2TeradataOp();
		}
		else if (Constants.DATA_MOVE_TYPE_H2V.equals(type))
		{
			return new Hdfs2VerticaOp();
		}
		else if (Constants.DATA_MOVE_TYPE_H2HB.equals(type))
		{
			return new Hdfs2HbaseOp();
		}
		else if (Constants.DATA_MOVE_TYPE_H2M.equals(type))
		{
			return new Hdfs2MySqlOp();
		}
		else if (Constants.DATA_MOVE_TYPE_T2H.equals(type))
		{
			return new Teradata2Hdfs();
		}
		else if (Constants.DATA_MOVE_TYPE_T2HB.equals(type))
		{
			return new Teradata2Hbase();
		}
		else if (Constants.DATA_MOVE_TYPE_O2H.equals(type))
		{
			return new Oracle2HdfsOp();
		}
		else if (Constants.DATA_MOVE_TYPE_O2HB.equals(type))
		{
			return new Oracle2HbaseOp();
		}
		else
		{
			return null;
		}
	}
}
