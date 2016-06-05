using NUnit.Framework;


namespace Org.Apache.Hadoop.Util
{
	public class TestProgress
	{
		[Fact]
		public virtual void TestSet()
		{
			Progress progress = new Progress();
			progress.Set(float.NaN);
			Assert.Equal(0, progress.GetProgress(), 0.0);
			progress.Set(float.NegativeInfinity);
			Assert.Equal(0, progress.GetProgress(), 0.0);
			progress.Set(-1);
			Assert.Equal(0, progress.GetProgress(), 0.0);
			progress.Set((float)1.1);
			Assert.Equal(1, progress.GetProgress(), 0.0);
			progress.Set(float.PositiveInfinity);
			Assert.Equal(1, progress.GetProgress(), 0.0);
		}
	}
}
