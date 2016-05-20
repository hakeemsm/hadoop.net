using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Interface which allows extracting information necessary to
	/// create schedulable identity strings.
	/// </summary>
	public interface Schedulable
	{
		org.apache.hadoop.security.UserGroupInformation getUserGroupInformation();
	}
}
