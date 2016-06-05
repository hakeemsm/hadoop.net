using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface Counter
	{
		string GetName();

		string GetDisplayName();

		long GetValue();

		void SetName(string name);

		void SetDisplayName(string displayName);

		void SetValue(long value);
	}
}
