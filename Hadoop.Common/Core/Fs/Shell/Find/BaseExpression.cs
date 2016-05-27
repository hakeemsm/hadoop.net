using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// Abstract expression for use in the
	/// <see cref="Find"/>
	/// command. Provides default
	/// behavior for a no-argument primary expression.
	/// </summary>
	public abstract class BaseExpression : Expression, Configurable
	{
		private string[] usage = new string[] { "Not yet implemented" };

		private string[] help = new string[] { "Not yet implemented" };

		/// <summary>
		/// Sets the usage text for this
		/// <see cref="Expression"/>
		/// 
		/// </summary>
		protected internal virtual void SetUsage(string[] usage)
		{
			this.usage = usage;
		}

		/// <summary>
		/// Sets the help text for this
		/// <see cref="Expression"/>
		/// 
		/// </summary>
		protected internal virtual void SetHelp(string[] help)
		{
			this.help = help;
		}

		public virtual string[] GetUsage()
		{
			return this.usage;
		}

		public virtual string[] GetHelp()
		{
			return this.help;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetOptions(FindOptions options)
		{
			this.options = options;
			foreach (Expression child in GetChildren())
			{
				child.SetOptions(options);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Prepare()
		{
			foreach (Expression child in GetChildren())
			{
				child.Prepare();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Finish()
		{
			foreach (Expression child in GetChildren())
			{
				child.Finish();
			}
		}

		/// <summary>
		/// Options passed in from the
		/// <see cref="Find"/>
		/// command.
		/// </summary>
		private FindOptions options;

		/// <summary>Hadoop configuration.</summary>
		private Configuration conf;

		/// <summary>Arguments for this expression.</summary>
		private List<string> arguments = new List<string>();

		/// <summary>Children of this expression.</summary>
		private List<Expression> children = new List<Expression>();

		/// <summary>Return the options to be used by this expression.</summary>
		protected internal virtual FindOptions GetOptions()
		{
			return (this.options == null) ? new FindOptions() : this.options;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return this.conf;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetType().Name);
			sb.Append("(");
			bool firstArg = true;
			foreach (string arg in GetArguments())
			{
				if (!firstArg)
				{
					sb.Append(",");
				}
				else
				{
					firstArg = false;
				}
				sb.Append(arg);
			}
			sb.Append(";");
			firstArg = true;
			foreach (Expression child in GetChildren())
			{
				if (!firstArg)
				{
					sb.Append(",");
				}
				else
				{
					firstArg = false;
				}
				sb.Append(child.ToString());
			}
			sb.Append(")");
			return sb.ToString();
		}

		public virtual bool IsAction()
		{
			foreach (Expression child in GetChildren())
			{
				if (child.IsAction())
				{
					return true;
				}
			}
			return false;
		}

		public virtual bool IsOperator()
		{
			return false;
		}

		/// <summary>Returns the arguments of this expression</summary>
		/// <returns>list of argument strings</returns>
		protected internal virtual IList<string> GetArguments()
		{
			return this.arguments;
		}

		/// <summary>Returns the argument at the given position (starting from 1).</summary>
		/// <param name="position">argument to be returned</param>
		/// <returns>requested argument</returns>
		/// <exception cref="System.IO.IOException">if the argument doesn't exist or is null</exception>
		protected internal virtual string GetArgument(int position)
		{
			if (position > this.arguments.Count)
			{
				throw new IOException("Missing argument at " + position);
			}
			string argument = this.arguments[position - 1];
			if (argument == null)
			{
				throw new IOException("Null argument at position " + position);
			}
			return argument;
		}

		/// <summary>Returns the children of this expression.</summary>
		/// <returns>list of child expressions</returns>
		protected internal virtual IList<Expression> GetChildren()
		{
			return this.children;
		}

		public virtual int GetPrecedence()
		{
			return 0;
		}

		public virtual void AddChildren(Deque<Expression> exprs)
		{
		}

		// no children by default, will be overridden by specific expressions.
		/// <summary>Add a specific number of children to this expression.</summary>
		/// <remarks>
		/// Add a specific number of children to this expression. The children are
		/// popped off the head of the expressions.
		/// </remarks>
		/// <param name="exprs">deque of expressions from which to take the children</param>
		/// <param name="count">number of children to be added</param>
		protected internal virtual void AddChildren(Deque<Expression> exprs, int count)
		{
			for (int i = 0; i < count; i++)
			{
				AddChild(exprs.Pop());
			}
		}

		/// <summary>Add a single argument to this expression.</summary>
		/// <remarks>
		/// Add a single argument to this expression. The argument is popped off the
		/// head of the expressions.
		/// </remarks>
		/// <param name="expr">child to add to the expression</param>
		private void AddChild(Expression expr)
		{
			children.Push(expr);
		}

		public virtual void AddArguments(Deque<string> args)
		{
		}

		// no children by default, will be overridden by specific expressions.
		/// <summary>Add a specific number of arguments to this expression.</summary>
		/// <remarks>
		/// Add a specific number of arguments to this expression. The children are
		/// popped off the head of the expressions.
		/// </remarks>
		/// <param name="args">deque of arguments from which to take the argument</param>
		/// <param name="count">number of children to be added</param>
		protected internal virtual void AddArguments(Deque<string> args, int count)
		{
			for (int i = 0; i < count; i++)
			{
				AddArgument(args.Pop());
			}
		}

		/// <summary>Add a single argument to this expression.</summary>
		/// <remarks>
		/// Add a single argument to this expression. The argument is popped off the
		/// head of the expressions.
		/// </remarks>
		/// <param name="arg">argument to add to the expression</param>
		protected internal virtual void AddArgument(string arg)
		{
			arguments.AddItem(arg);
		}

		/// <summary>
		/// Returns the
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// from the
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item. If the
		/// current options require links to be followed then the returned file status
		/// is that of the linked file.
		/// </summary>
		/// <param name="item">PathData</param>
		/// <param name="depth">current depth in the process directories</param>
		/// <returns>FileStatus</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileStatus GetFileStatus(PathData item, int depth)
		{
			FileStatus fileStatus = item.stat;
			if (fileStatus.IsSymlink())
			{
				if (options.IsFollowLink() || (options.IsFollowArgLink() && (depth == 0)))
				{
					Path linkedFile = item.fs.ResolvePath(fileStatus.GetSymlink());
					fileStatus = GetFileSystem(item).GetFileStatus(linkedFile);
				}
			}
			return fileStatus;
		}

		/// <summary>
		/// Returns the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// from the
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item.
		/// </summary>
		/// <param name="item">PathData</param>
		/// <returns>Path</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Path GetPath(PathData item)
		{
			return item.path;
		}

		/// <summary>
		/// Returns the
		/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
		/// associated with the
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item.
		/// </summary>
		/// <param name="item">PathData</param>
		/// <returns>FileSystem</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem GetFileSystem(PathData item)
		{
			return item.fs;
		}

		public abstract Result Apply(PathData arg1, int arg2);
	}
}
