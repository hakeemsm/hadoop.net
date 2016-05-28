using System.Collections.Generic;
using System.IO;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// This class wraps a list of problems with the input, so that the user
	/// can get a list of problems together instead of finding and fixing them one
	/// by one.
	/// </summary>
	[System.Serializable]
	public class InvalidInputException : IOException
	{
		private const long serialVersionUID = -380668190578456802L;

		private IList<IOException> problems;

		/// <summary>Create the exception with the given list.</summary>
		/// <param name="probs">the list of problems to report. this list is not copied.</param>
		public InvalidInputException(IList<IOException> probs)
		{
			problems = probs;
		}

		/// <summary>Get the complete list of the problems reported.</summary>
		/// <returns>the list of problems, which must not be modified</returns>
		public virtual IList<IOException> GetProblems()
		{
			return problems;
		}

		/// <summary>Get a summary message of the problems found.</summary>
		/// <returns>the concatenated messages from all of the problems.</returns>
		public override string Message
		{
			get
			{
				StringBuilder result = new StringBuilder();
				IEnumerator<IOException> itr = problems.GetEnumerator();
				while (itr.HasNext())
				{
					result.Append(itr.Next().Message);
					if (itr.HasNext())
					{
						result.Append("\n");
					}
				}
				return result.ToString();
			}
		}
	}
}
