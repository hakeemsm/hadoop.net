using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Abstract expression for use in the
	/// <see cref="Find"/>
	/// command. Provides default
	/// behavior for a no-argument primary expression.
	/// </summary>
	public abstract class BaseExpression : org.apache.hadoop.fs.shell.find.Expression
		, org.apache.hadoop.conf.Configurable
	{
		private string[] usage = new string[] { "Not yet implemented" };

		private string[] help = new string[] { "Not yet implemented" };

		/// <summary>
		/// Sets the usage text for this
		/// <see cref="Expression"/>
		/// 
		/// </summary>
		protected internal virtual void setUsage(string[] usage)
		{
			this.usage = usage;
		}

		/// <summary>
		/// Sets the help text for this
		/// <see cref="Expression"/>
		/// 
		/// </summary>
		protected internal virtual void setHelp(string[] help)
		{
			this.help = help;
		}

		public virtual string[] getUsage()
		{
			return this.usage;
		}

		public virtual string[] getHelp()
		{
			return this.help;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void setOptions(org.apache.hadoop.fs.shell.find.FindOptions options
			)
		{
			this.options = options;
			foreach (org.apache.hadoop.fs.shell.find.Expression child in getChildren())
			{
				child.setOptions(options);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void prepare()
		{
			foreach (org.apache.hadoop.fs.shell.find.Expression child in getChildren())
			{
				child.prepare();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void finish()
		{
			foreach (org.apache.hadoop.fs.shell.find.Expression child in getChildren())
			{
				child.finish();
			}
		}

		/// <summary>
		/// Options passed in from the
		/// <see cref="Find"/>
		/// command.
		/// </summary>
		private org.apache.hadoop.fs.shell.find.FindOptions options;

		/// <summary>Hadoop configuration.</summary>
		private org.apache.hadoop.conf.Configuration conf;

		/// <summary>Arguments for this expression.</summary>
		private System.Collections.Generic.LinkedList<string> arguments = new System.Collections.Generic.LinkedList
			<string>();

		/// <summary>Children of this expression.</summary>
		private System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.find.Expression
			> children = new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.find.Expression
			>();

		/// <summary>Return the options to be used by this expression.</summary>
		protected internal virtual org.apache.hadoop.fs.shell.find.FindOptions getOptions
			()
		{
			return (this.options == null) ? new org.apache.hadoop.fs.shell.find.FindOptions()
				 : this.options;
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return this.conf;
		}

		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			sb.Append(Sharpen.Runtime.getClassForObject(this).getSimpleName());
			sb.Append("(");
			bool firstArg = true;
			foreach (string arg in getArguments())
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
			foreach (org.apache.hadoop.fs.shell.find.Expression child in getChildren())
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

		public virtual bool isAction()
		{
			foreach (org.apache.hadoop.fs.shell.find.Expression child in getChildren())
			{
				if (child.isAction())
				{
					return true;
				}
			}
			return false;
		}

		public virtual bool isOperator()
		{
			return false;
		}

		/// <summary>Returns the arguments of this expression</summary>
		/// <returns>list of argument strings</returns>
		protected internal virtual System.Collections.Generic.IList<string> getArguments(
			)
		{
			return this.arguments;
		}

		/// <summary>Returns the argument at the given position (starting from 1).</summary>
		/// <param name="position">argument to be returned</param>
		/// <returns>requested argument</returns>
		/// <exception cref="System.IO.IOException">if the argument doesn't exist or is null</exception>
		protected internal virtual string getArgument(int position)
		{
			if (position > this.arguments.Count)
			{
				throw new System.IO.IOException("Missing argument at " + position);
			}
			string argument = this.arguments[position - 1];
			if (argument == null)
			{
				throw new System.IO.IOException("Null argument at position " + position);
			}
			return argument;
		}

		/// <summary>Returns the children of this expression.</summary>
		/// <returns>list of child expressions</returns>
		protected internal virtual System.Collections.Generic.IList<org.apache.hadoop.fs.shell.find.Expression
			> getChildren()
		{
			return this.children;
		}

		public virtual int getPrecedence()
		{
			return 0;
		}

		public virtual void addChildren(java.util.Deque<org.apache.hadoop.fs.shell.find.Expression
			> exprs)
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
		protected internal virtual void addChildren(java.util.Deque<org.apache.hadoop.fs.shell.find.Expression
			> exprs, int count)
		{
			for (int i = 0; i < count; i++)
			{
				addChild(exprs.pop());
			}
		}

		/// <summary>Add a single argument to this expression.</summary>
		/// <remarks>
		/// Add a single argument to this expression. The argument is popped off the
		/// head of the expressions.
		/// </remarks>
		/// <param name="expr">child to add to the expression</param>
		private void addChild(org.apache.hadoop.fs.shell.find.Expression expr)
		{
			children.push(expr);
		}

		public virtual void addArguments(java.util.Deque<string> args)
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
		protected internal virtual void addArguments(java.util.Deque<string> args, int count
			)
		{
			for (int i = 0; i < count; i++)
			{
				addArgument(args.pop());
			}
		}

		/// <summary>Add a single argument to this expression.</summary>
		/// <remarks>
		/// Add a single argument to this expression. The argument is popped off the
		/// head of the expressions.
		/// </remarks>
		/// <param name="arg">argument to add to the expression</param>
		protected internal virtual void addArgument(string arg)
		{
			arguments.add(arg);
		}

		/// <summary>
		/// Returns the
		/// <see cref="org.apache.hadoop.fs.FileStatus"/>
		/// from the
		/// <see cref="org.apache.hadoop.fs.shell.PathData"/>
		/// item. If the
		/// current options require links to be followed then the returned file status
		/// is that of the linked file.
		/// </summary>
		/// <param name="item">PathData</param>
		/// <param name="depth">current depth in the process directories</param>
		/// <returns>FileStatus</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.shell.PathData
			 item, int depth)
		{
			org.apache.hadoop.fs.FileStatus fileStatus = item.stat;
			if (fileStatus.isSymlink())
			{
				if (options.isFollowLink() || (options.isFollowArgLink() && (depth == 0)))
				{
					org.apache.hadoop.fs.Path linkedFile = item.fs.resolvePath(fileStatus.getSymlink(
						));
					fileStatus = getFileSystem(item).getFileStatus(linkedFile);
				}
			}
			return fileStatus;
		}

		/// <summary>
		/// Returns the
		/// <see cref="org.apache.hadoop.fs.Path"/>
		/// from the
		/// <see cref="org.apache.hadoop.fs.shell.PathData"/>
		/// item.
		/// </summary>
		/// <param name="item">PathData</param>
		/// <returns>Path</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Path getPath(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			return item.path;
		}

		/// <summary>
		/// Returns the
		/// <see cref="org.apache.hadoop.fs.FileSystem"/>
		/// associated with the
		/// <see cref="org.apache.hadoop.fs.shell.PathData"/>
		/// item.
		/// </summary>
		/// <param name="item">PathData</param>
		/// <returns>FileSystem</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.FileSystem getFileSystem(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			return item.fs;
		}

		public abstract org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
			 arg1, int arg2);
	}
}
