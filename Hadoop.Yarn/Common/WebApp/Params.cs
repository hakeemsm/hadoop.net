using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>Public static constants for webapp parameters.</summary>
	/// <remarks>
	/// Public static constants for webapp parameters. Do NOT put any
	/// private or application specific constants here as they're part of
	/// the API for users of the controllers and views.
	/// </remarks>
	public abstract class Params
	{
		public const string Title = "title";

		public const string TitleLink = "title.href";

		public const string User = "user";

		public const string ErrorDetails = "error.details";
	}

	public static class ParamsConstants
	{
	}
}
