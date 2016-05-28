using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao
{
	public class AMAttemptsInfo
	{
		protected internal AList<AMAttemptInfo> attempt = new AList<AMAttemptInfo>();

		public AMAttemptsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(AMAttemptInfo info)
		{
			this.attempt.AddItem(info);
		}

		public virtual AList<AMAttemptInfo> GetAttempts()
		{
			return this.attempt;
		}
	}
}
