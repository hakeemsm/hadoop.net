using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	public class TestXDR
	{
		internal const int WriteValue = 23;

		private void SerializeInt(int times)
		{
			XDR w = new XDR();
			for (int i = 0; i < times; ++i)
			{
				w.WriteInt(WriteValue);
			}
			XDR r = w.AsReadOnlyWrap();
			for (int i_1 = 0; i_1 < times; ++i_1)
			{
				Assert.Equal(WriteValue, r.ReadInt());
			}
		}

		private void SerializeLong(int times)
		{
			XDR w = new XDR();
			for (int i = 0; i < times; ++i)
			{
				w.WriteLongAsHyper(WriteValue);
			}
			XDR r = w.AsReadOnlyWrap();
			for (int i_1 = 0; i_1 < times; ++i_1)
			{
				Assert.Equal(WriteValue, r.ReadHyper());
			}
		}

		[Fact]
		public virtual void TestPerformance()
		{
			int TestTimes = 8 << 20;
			SerializeInt(TestTimes);
			SerializeLong(TestTimes);
		}
	}
}
