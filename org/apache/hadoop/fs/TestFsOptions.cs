using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFsOptions
	{
		[NUnit.Framework.Test]
		public virtual void testProcessChecksumOpt()
		{
			org.apache.hadoop.fs.Options.ChecksumOpt defaultOpt = new org.apache.hadoop.fs.Options.ChecksumOpt
				(org.apache.hadoop.util.DataChecksum.Type.CRC32, 512);
			org.apache.hadoop.fs.Options.ChecksumOpt finalOpt;
			// Give a null 
			finalOpt = org.apache.hadoop.fs.Options.ChecksumOpt.processChecksumOpt(defaultOpt
				, null);
			checkParams(defaultOpt, finalOpt);
			// null with bpc
			finalOpt = org.apache.hadoop.fs.Options.ChecksumOpt.processChecksumOpt(defaultOpt
				, null, 1024);
			checkParams(org.apache.hadoop.util.DataChecksum.Type.CRC32, 1024, finalOpt);
			org.apache.hadoop.fs.Options.ChecksumOpt myOpt = new org.apache.hadoop.fs.Options.ChecksumOpt
				();
			// custom with unspecified parameters
			finalOpt = org.apache.hadoop.fs.Options.ChecksumOpt.processChecksumOpt(defaultOpt
				, myOpt);
			checkParams(defaultOpt, finalOpt);
			myOpt = new org.apache.hadoop.fs.Options.ChecksumOpt(org.apache.hadoop.util.DataChecksum.Type
				.CRC32C, 2048);
			// custom config
			finalOpt = org.apache.hadoop.fs.Options.ChecksumOpt.processChecksumOpt(defaultOpt
				, myOpt);
			checkParams(org.apache.hadoop.util.DataChecksum.Type.CRC32C, 2048, finalOpt);
			// custom config + bpc
			finalOpt = org.apache.hadoop.fs.Options.ChecksumOpt.processChecksumOpt(defaultOpt
				, myOpt, 4096);
			checkParams(org.apache.hadoop.util.DataChecksum.Type.CRC32C, 4096, finalOpt);
		}

		private void checkParams(org.apache.hadoop.fs.Options.ChecksumOpt expected, org.apache.hadoop.fs.Options.ChecksumOpt
			 obtained)
		{
			NUnit.Framework.Assert.AreEqual(expected.getChecksumType(), obtained.getChecksumType
				());
			NUnit.Framework.Assert.AreEqual(expected.getBytesPerChecksum(), obtained.getBytesPerChecksum
				());
		}

		private void checkParams(org.apache.hadoop.util.DataChecksum.Type type, int bpc, 
			org.apache.hadoop.fs.Options.ChecksumOpt obtained)
		{
			NUnit.Framework.Assert.AreEqual(type, obtained.getChecksumType());
			NUnit.Framework.Assert.AreEqual(bpc, obtained.getBytesPerChecksum());
		}
	}
}
