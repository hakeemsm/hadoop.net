using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Simple class that holds the number formatters used in the slive application
	/// 	</summary>
	internal class Formatter
	{
		private const string NumberFormat = "###.###";

		private static NumberFormat decFormatter = null;

		private static NumberFormat percFormatter = null;

		/// <summary>No construction allowed - only simple static accessor functions</summary>
		private Formatter()
		{
		}

		/// <summary>Gets a decimal formatter that has 3 decimal point precision</summary>
		/// <returns>NumberFormat formatter</returns>
		internal static NumberFormat GetDecimalFormatter()
		{
			lock (typeof(Formatter))
			{
				if (decFormatter == null)
				{
					decFormatter = new DecimalFormat(NumberFormat);
				}
				return decFormatter;
			}
		}

		/// <summary>Gets a percent formatter that has 3 decimal point precision</summary>
		/// <returns>NumberFormat formatter</returns>
		internal static NumberFormat GetPercentFormatter()
		{
			lock (typeof(Formatter))
			{
				if (percFormatter == null)
				{
					percFormatter = NumberFormat.GetPercentInstance();
					percFormatter.SetMaximumFractionDigits(3);
				}
				return percFormatter;
			}
		}
	}
}
