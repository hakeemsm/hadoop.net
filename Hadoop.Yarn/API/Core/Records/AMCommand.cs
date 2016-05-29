using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// Command sent by the Resource Manager to the Application Master in the
	/// AllocateResponse
	/// </summary>
	public enum AMCommand
	{
		AmResync,
		AmShutdown
	}
}
