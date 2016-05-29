using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	/// <summary>Constants used in both Client and Application Master</summary>
	public class DSConstants
	{
		/// <summary>Environment key name pointing to the shell script's location</summary>
		public const string Distributedshellscriptlocation = "DISTRIBUTEDSHELLSCRIPTLOCATION";

		/// <summary>Environment key name denoting the file timestamp for the shell script.</summary>
		/// <remarks>
		/// Environment key name denoting the file timestamp for the shell script.
		/// Used to validate the local resource.
		/// </remarks>
		public const string Distributedshellscripttimestamp = "DISTRIBUTEDSHELLSCRIPTTIMESTAMP";

		/// <summary>Environment key name denoting the file content length for the shell script.
		/// 	</summary>
		/// <remarks>
		/// Environment key name denoting the file content length for the shell script.
		/// Used to validate the local resource.
		/// </remarks>
		public const string Distributedshellscriptlen = "DISTRIBUTEDSHELLSCRIPTLEN";

		/// <summary>Environment key name denoting the timeline domain ID.</summary>
		public const string Distributedshelltimelinedomain = "DISTRIBUTEDSHELLTIMELINEDOMAIN";
	}
}
