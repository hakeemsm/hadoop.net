using System;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class PipeApplicationRunnableStub : CommonStub
	{
		/*
		Stub for  TestPipeApplication   test. This stub produced test data for main test. Main test  checks data
		*/
		public static void Main(string[] args)
		{
			PipeApplicationRunnableStub client = new PipeApplicationRunnableStub();
			client.BinaryProtocolStub();
		}

		public virtual void BinaryProtocolStub()
		{
			try
			{
				InitSoket();
				System.Console.Out.WriteLine("start OK");
				// RUN_MAP.code
				// should be 3
				int answer = WritableUtils.ReadVInt(dataInput);
				System.Console.Out.WriteLine("RunMap:" + answer);
				TestPipeApplication.FakeSplit split = new TestPipeApplication.FakeSplit();
				ReadObject(split, dataInput);
				WritableUtils.ReadVInt(dataInput);
				WritableUtils.ReadVInt(dataInput);
				// end runMap
				// get InputTypes
				WritableUtils.ReadVInt(dataInput);
				string inText = Text.ReadString(dataInput);
				System.Console.Out.WriteLine("Key class:" + inText);
				inText = Text.ReadString(dataInput);
				System.Console.Out.WriteLine("Value class:" + inText);
				int inCode = 0;
				// read all data from sender and write to output
				while ((inCode = WritableUtils.ReadVInt(dataInput)) == 4)
				{
					FloatWritable key = new FloatWritable();
					NullWritable value = NullWritable.Get();
					ReadObject(key, dataInput);
					System.Console.Out.WriteLine("value:" + key.Get());
					ReadObject(value, dataInput);
				}
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
