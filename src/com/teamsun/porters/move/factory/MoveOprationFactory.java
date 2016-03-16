package com.teamsun.porters.move.factory;

import com.teamsun.porters.move.op.MoveOpration;
import com.teamsun.porters.move.op.hdfs.Hdfs2HbaseOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2HdfsOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2MySqlOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2OracleOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2TeradataOp;
import com.teamsun.porters.move.op.hdfs.Hdfs2VerticaOp;
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
		else
		{
			return null;
		}
	}
}
