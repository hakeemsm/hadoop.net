using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Hash
{
	public class TestHash
	{
		internal const string Line = "34563@45kjkksdf/ljfdb9d8fbusd*89uggjsk<dfgjsdfh@sddc2q3esc";

		[NUnit.Framework.Test]
		public virtual void TestHash()
		{
			int iterations = 30;
			NUnit.Framework.Assert.IsTrue("testHash jenkins error !!!", Org.Apache.Hadoop.Util.Hash.Hash
				.JenkinsHash == Org.Apache.Hadoop.Util.Hash.Hash.ParseHashType("jenkins"));
			NUnit.Framework.Assert.IsTrue("testHash murmur error !!!", Org.Apache.Hadoop.Util.Hash.Hash
				.MurmurHash == Org.Apache.Hadoop.Util.Hash.Hash.ParseHashType("murmur"));
			NUnit.Framework.Assert.IsTrue("testHash undefined", Org.Apache.Hadoop.Util.Hash.Hash
				.InvalidHash == Org.Apache.Hadoop.Util.Hash.Hash.ParseHashType("undefined"));
			Configuration cfg = new Configuration();
			cfg.Set("hadoop.util.hash.type", "murmur");
			NUnit.Framework.Assert.IsTrue("testHash", MurmurHash.GetInstance() == Org.Apache.Hadoop.Util.Hash.Hash
				.GetInstance(cfg));
			cfg = new Configuration();
			cfg.Set("hadoop.util.hash.type", "jenkins");
			NUnit.Framework.Assert.IsTrue("testHash jenkins configuration error !!!", JenkinsHash
				.GetInstance() == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(cfg));
			cfg = new Configuration();
			NUnit.Framework.Assert.IsTrue("testHash undefine configuration error !!!", MurmurHash
				.GetInstance() == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(cfg));
			NUnit.Framework.Assert.IsTrue("testHash error jenkin getInstance !!!", JenkinsHash
				.GetInstance() == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
				.JenkinsHash));
			NUnit.Framework.Assert.IsTrue("testHash error murmur getInstance !!!", MurmurHash
				.GetInstance() == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
				.MurmurHash));
			NUnit.Framework.Assert.IsNull("testHash error invalid getInstance !!!", Org.Apache.Hadoop.Util.Hash.Hash
				.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash.InvalidHash));
			int murmurHash = Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
				.MurmurHash).Hash(Sharpen.Runtime.GetBytesForString(Line));
			for (int i = 0; i < iterations; i++)
			{
				NUnit.Framework.Assert.IsTrue("multiple evaluation murmur hash error !!!", murmurHash
					 == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
					.MurmurHash).Hash(Sharpen.Runtime.GetBytesForString(Line)));
			}
			murmurHash = Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
				.MurmurHash).Hash(Sharpen.Runtime.GetBytesForString(Line), 67);
			for (int i_1 = 0; i_1 < iterations; i_1++)
			{
				NUnit.Framework.Assert.IsTrue("multiple evaluation murmur hash error !!!", murmurHash
					 == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
					.MurmurHash).Hash(Sharpen.Runtime.GetBytesForString(Line), 67));
			}
			int jenkinsHash = Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
				.JenkinsHash).Hash(Sharpen.Runtime.GetBytesForString(Line));
			for (int i_2 = 0; i_2 < iterations; i_2++)
			{
				NUnit.Framework.Assert.IsTrue("multiple evaluation jenkins hash error !!!", jenkinsHash
					 == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
					.JenkinsHash).Hash(Sharpen.Runtime.GetBytesForString(Line)));
			}
			jenkinsHash = Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
				.JenkinsHash).Hash(Sharpen.Runtime.GetBytesForString(Line), 67);
			for (int i_3 = 0; i_3 < iterations; i_3++)
			{
				NUnit.Framework.Assert.IsTrue("multiple evaluation jenkins hash error !!!", jenkinsHash
					 == Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(Org.Apache.Hadoop.Util.Hash.Hash
					.JenkinsHash).Hash(Sharpen.Runtime.GetBytesForString(Line), 67));
			}
		}
	}
}
