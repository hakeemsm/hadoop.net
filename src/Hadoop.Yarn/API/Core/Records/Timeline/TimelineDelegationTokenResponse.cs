using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>The response of delegation token related request</summary>
	public class TimelineDelegationTokenResponse
	{
		private string type;

		private object content;

		public TimelineDelegationTokenResponse()
		{
		}

		public virtual string GetType()
		{
			return type;
		}

		public virtual void SetType(string type)
		{
			this.type = type;
		}

		public virtual object GetContent()
		{
			return content;
		}

		public virtual void SetContent(object content)
		{
			this.content = content;
		}
	}
}
