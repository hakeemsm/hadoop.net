using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestIFile
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIFileWriterWithCodec()
		{
			Configuration conf = new Configuration();
			FileSystem localFs = FileSystem.GetLocal(conf);
			FileSystem rfs = ((LocalFileSystem)localFs).GetRaw();
			Path path = new Path(new Path("build/test.ifile"), "data");
			DefaultCodec codec = new GzipCodec();
			codec.SetConf(conf);
			IFile.Writer<Text, Text> writer = new IFile.Writer<Text, Text>(conf, rfs.Create(path
				), typeof(Text), typeof(Text), codec, null);
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIFileReaderWithCodec()
		{
			Configuration conf = new Configuration();
			FileSystem localFs = FileSystem.GetLocal(conf);
			FileSystem rfs = ((LocalFileSystem)localFs).GetRaw();
			Path path = new Path(new Path("build/test.ifile"), "data");
			DefaultCodec codec = new GzipCodec();
			codec.SetConf(conf);
			FSDataOutputStream @out = rfs.Create(path);
			IFile.Writer<Text, Text> writer = new IFile.Writer<Text, Text>(conf, @out, typeof(
				Text), typeof(Text), codec, null);
			writer.Close();
			FSDataInputStream @in = rfs.Open(path);
			IFile.Reader<Text, Text> reader = new IFile.Reader<Text, Text>(conf, @in, rfs.GetFileStatus
				(path).GetLen(), codec, null);
			reader.Close();
			// test check sum 
			byte[] ab = new byte[100];
			int readed = reader.checksumIn.ReadWithChecksum(ab, 0, ab.Length);
			NUnit.Framework.Assert.AreEqual(readed, reader.checksumIn.GetChecksum().Length);
		}
	}
}
