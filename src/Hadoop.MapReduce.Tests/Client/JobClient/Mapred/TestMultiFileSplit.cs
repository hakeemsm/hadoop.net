using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>test MultiFileSplit class</summary>
	public class TestMultiFileSplit : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestReadWrite()
		{
			MultiFileSplit split = new MultiFileSplit(new JobConf(), new Path[] { new Path("/test/path/1"
				), new Path("/test/path/2") }, new long[] { 100, 200 });
			ByteArrayOutputStream bos = null;
			byte[] result = null;
			try
			{
				bos = new ByteArrayOutputStream();
				split.Write(new DataOutputStream(bos));
				result = bos.ToByteArray();
			}
			finally
			{
				IOUtils.CloseStream(bos);
			}
			MultiFileSplit readSplit = new MultiFileSplit();
			ByteArrayInputStream bis = null;
			try
			{
				bis = new ByteArrayInputStream(result);
				readSplit.ReadFields(new DataInputStream(bis));
			}
			finally
			{
				IOUtils.CloseStream(bis);
			}
			NUnit.Framework.Assert.IsTrue(split.GetLength() != 0);
			NUnit.Framework.Assert.AreEqual(split.GetLength(), readSplit.GetLength());
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(split.GetPaths(), readSplit.GetPaths(
				)));
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(split.GetLengths(), readSplit.GetLengths
				()));
			System.Console.Out.WriteLine(split.ToString());
		}

		/// <summary>test method getLocations</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetLocations()
		{
			JobConf job = new JobConf();
			FilePath tmpFile = FilePath.CreateTempFile("test", "txt");
			tmpFile.CreateNewFile();
			OutputStream @out = new FileOutputStream(tmpFile);
			@out.Write(Sharpen.Runtime.GetBytesForString("tempfile"));
			@out.Flush();
			@out.Close();
			Path[] path = new Path[] { new Path(tmpFile.GetAbsolutePath()) };
			long[] lengths = new long[] { 100 };
			MultiFileSplit split = new MultiFileSplit(job, path, lengths);
			string[] locations = split.GetLocations();
			NUnit.Framework.Assert.IsTrue(locations.Length == 1);
			NUnit.Framework.Assert.AreEqual(locations[0], "localhost");
		}
	}
}
