using System;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class PipeReducerStub : CommonStub
	{
		/*
		Stub for  TestPipeApplication   test. This stub produced test data for main test. Main test  checks data
		*/
		public static void Main(string[] args)
		{
			PipeReducerStub client = new PipeReducerStub();
			client.BinaryProtocolStub();
		}

		public virtual void BinaryProtocolStub()
		{
			try
			{
				InitSoket();
				//should be 5
				//RUN_REDUCE boolean 
				WritableUtils.ReadVInt(dataInput);
				WritableUtils.ReadVInt(dataInput);
				int intValue = WritableUtils.ReadVInt(dataInput);
				System.Console.Out.WriteLine("getIsJavaRecordWriter:" + intValue);
				// reduce key
				WritableUtils.ReadVInt(dataInput);
				// value of reduce key
				BooleanWritable value = new BooleanWritable();
				ReadObject(value, dataInput);
				System.Console.Out.WriteLine("reducer key :" + value);
				// reduce value code:
				// reduce values
				while ((intValue = WritableUtils.ReadVInt(dataInput)) == 7)
				{
					Text txt = new Text();
					// value
					ReadObject(txt, dataInput);
					System.Console.Out.WriteLine("reduce value  :" + txt);
				}
				// done
				WritableUtils.WriteVInt(dataOut, 54);
				dataOut.Flush();
				dataOut.Close();
			}
			catch (Exception x)
			{
				Sharpen.Runtime.PrintStackTrace(x);
			}
			finally
			{
				CloseSoket();
			}
		}
	}
}
