using System;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class PipeApplicationStub : CommonStub
	{
		/*
		Stub for  TestPipeApplication   test. This stub produced test data for main test. Main test  checks data
		*/
		public static void Main(string[] args)
		{
			PipeApplicationStub client = new PipeApplicationStub();
			client.BinaryProtocolStub();
		}

		public virtual void BinaryProtocolStub()
		{
			try
			{
				InitSoket();
				// output code
				WritableUtils.WriteVInt(dataOut, 50);
				IntWritable wt = new IntWritable();
				wt.Set(123);
				WriteObject(wt, dataOut);
				WriteObject(new Text("value"), dataOut);
				//  PARTITIONED_OUTPUT
				WritableUtils.WriteVInt(dataOut, 51);
				WritableUtils.WriteVInt(dataOut, 0);
				WriteObject(wt, dataOut);
				WriteObject(new Text("value"), dataOut);
				// STATUS
				WritableUtils.WriteVInt(dataOut, 52);
				Text.WriteString(dataOut, "PROGRESS");
				dataOut.Flush();
				// progress
				WritableUtils.WriteVInt(dataOut, 53);
				dataOut.WriteFloat(0.55f);
				// register counter
				WritableUtils.WriteVInt(dataOut, 55);
				// id
				WritableUtils.WriteVInt(dataOut, 0);
				Text.WriteString(dataOut, "group");
				Text.WriteString(dataOut, "name");
				// increment counter
				WritableUtils.WriteVInt(dataOut, 56);
				WritableUtils.WriteVInt(dataOut, 0);
				WritableUtils.WriteVLong(dataOut, 2);
				// map item
				int intValue = WritableUtils.ReadVInt(dataInput);
				System.Console.Out.WriteLine("intValue:" + intValue);
				IntWritable iw = new IntWritable();
				ReadObject(iw, dataInput);
				System.Console.Out.WriteLine("key:" + iw.Get());
				Text txt = new Text();
				ReadObject(txt, dataInput);
				System.Console.Out.WriteLine("value:" + txt.ToString());
				// done
				// end of session
				WritableUtils.WriteVInt(dataOut, 54);
				System.Console.Out.WriteLine("finish");
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
