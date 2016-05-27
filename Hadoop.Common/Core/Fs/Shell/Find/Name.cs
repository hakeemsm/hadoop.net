using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// Implements the -name expression for the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class Name : BaseExpression
	{
		/// <summary>Registers this expression with the specified factory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void RegisterExpression(ExpressionFactory factory)
		{
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Find.Name), "-name");
			factory.AddClass(typeof(Name.Iname), "-iname");
		}

		private static readonly string[] Usage = new string[] { "-name pattern", "-iname pattern"
			 };

		private static readonly string[] Help = new string[] { "Evaluates as true if the basename of the file matches the"
			, "pattern using standard file system globbing.", "If -iname is used then the match is case insensitive."
			 };

		private GlobPattern globPattern;

		private bool caseSensitive = true;

		/// <summary>Creates a case sensitive name expression.</summary>
		public Name()
			: this(true)
		{
		}

		/// <summary>
		/// Construct a Name
		/// <see cref="Expression"/>
		/// with a specified case sensitivity.
		/// </summary>
		/// <param name="caseSensitive">if true the comparisons are case sensitive.</param>
		private Name(bool caseSensitive)
			: base()
		{
			SetUsage(Usage);
			SetHelp(Help);
			SetCaseSensitive(caseSensitive);
		}

		private void SetCaseSensitive(bool caseSensitive)
		{
			this.caseSensitive = caseSensitive;
		}

		public override void AddArguments(Deque<string> args)
		{
			AddArguments(args, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Prepare()
		{
			string argPattern = GetArgument(1);
			if (!caseSensitive)
			{
				argPattern = StringUtils.ToLowerCase(argPattern);
			}
			globPattern = new GlobPattern(argPattern);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Result Apply(PathData item, int depth)
		{
			string name = GetPath(item).GetName();
			if (!caseSensitive)
			{
				name = StringUtils.ToLowerCase(name);
			}
			if (globPattern.Matches(name))
			{
				return Result.Pass;
			}
			else
			{
				return Result.Fail;
			}
		}

		/// <summary>Case insensitive version of the -name expression.</summary>
		internal class Iname : FilterExpression
		{
			public Iname()
				: base(new Name(false))
			{
			}
		}
	}
}
