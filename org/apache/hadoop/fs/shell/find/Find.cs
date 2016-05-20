using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class Find : org.apache.hadoop.fs.shell.FsCommand
	{
		/// <summary>Register the names for the count command</summary>
		/// <param name="factory">the command factory that will instantiate this class</param>
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Find
				)), "-find");
		}

		public const string NAME = "find";

		public const string USAGE = "<path> ... <expression> ...";

		public static readonly string DESCRIPTION;

		private static string[] HELP = new string[] { "Finds all files that match the specified expression and"
			, "applies selected actions to them. If no <path> is specified", "then defaults to the current working directory. If no"
			, "expression is specified then defaults to -print." };

		private const string OPTION_FOLLOW_LINK = "L";

		private const string OPTION_FOLLOW_ARG_LINK = "H";

		/// <summary>List of expressions recognized by this command.</summary>
		private static readonly System.Collections.Generic.ICollection<java.lang.Class> EXPRESSIONS
			 = new java.util.HashSet<java.lang.Class>();

		private static void addExpression(java.lang.Class clazz)
		{
			EXPRESSIONS.add(clazz.asSubclass<org.apache.hadoop.fs.shell.find.Expression>());
		}

		static Find()
		{
			// Initialize the static variables.
			// Operator Expressions
			addExpression(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.And
				)));
			// Action Expressions
			addExpression(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Print
				)));
			// Navigation Expressions
			// Matcher Expressions
			addExpression(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Name
				)));
			DESCRIPTION = buildDescription(org.apache.hadoop.fs.shell.find.ExpressionFactory.
				getExpressionFactory());
			// Register the expressions with the expression factory.
			registerExpressions(org.apache.hadoop.fs.shell.find.ExpressionFactory.getExpressionFactory
				());
		}

		/// <summary>Options for use in this command</summary>
		private org.apache.hadoop.fs.shell.find.FindOptions options;

		/// <summary>Root expression for this instance of the command.</summary>
		private org.apache.hadoop.fs.shell.find.Expression rootExpression;

		/// <summary>
		/// Set of path items returning a
		/// <see cref="Result.STOP"/>
		/// result.
		/// </summary>
		private java.util.HashSet<org.apache.hadoop.fs.Path> stopPaths = new java.util.HashSet
			<org.apache.hadoop.fs.Path>();

		/// <summary>Register the expressions with the expression factory.</summary>
		private static void registerExpressions(org.apache.hadoop.fs.shell.find.ExpressionFactory
			 factory)
		{
			foreach (java.lang.Class exprClass in EXPRESSIONS)
			{
				factory.registerExpression(exprClass);
			}
		}

		/// <summary>Build the description used by the help command.</summary>
		private static string buildDescription(org.apache.hadoop.fs.shell.find.ExpressionFactory
			 factory)
		{
			System.Collections.Generic.List<org.apache.hadoop.fs.shell.find.Expression> operators
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.shell.find.Expression
				>();
			System.Collections.Generic.List<org.apache.hadoop.fs.shell.find.Expression> primaries
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.shell.find.Expression
				>();
			foreach (java.lang.Class exprClass in EXPRESSIONS)
			{
				org.apache.hadoop.fs.shell.find.Expression expr = factory.createExpression(exprClass
					, null);
				if (expr.isOperator())
				{
					operators.add(expr);
				}
				else
				{
					primaries.add(expr);
				}
			}
			operators.Sort(new _Comparator_120());
			primaries.Sort(new _Comparator_126());
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			foreach (string line in HELP)
			{
				sb.Append(line).Append("\n");
			}
			sb.Append("\n");
			sb.Append("The following primary expressions are recognised:\n");
			foreach (org.apache.hadoop.fs.shell.find.Expression expr_1 in primaries)
			{
				foreach (string line_1 in expr_1.getUsage())
				{
					sb.Append("  ").Append(line_1).Append("\n");
				}
				foreach (string line_2 in expr_1.getHelp())
				{
					sb.Append("    ").Append(line_2).Append("\n");
				}
				sb.Append("\n");
			}
			sb.Append("The following operators are recognised:\n");
			foreach (org.apache.hadoop.fs.shell.find.Expression expr_2 in operators)
			{
				foreach (string line_1 in expr_2.getUsage())
				{
					sb.Append("  ").Append(line_1).Append("\n");
				}
				foreach (string line_2 in expr_2.getHelp())
				{
					sb.Append("    ").Append(line_2).Append("\n");
				}
				sb.Append("\n");
			}
			return sb.ToString();
		}

		private sealed class _Comparator_120 : java.util.Comparator<org.apache.hadoop.fs.shell.find.Expression
			>
		{
			public _Comparator_120()
			{
			}

			public int compare(org.apache.hadoop.fs.shell.find.Expression arg0, org.apache.hadoop.fs.shell.find.Expression
				 arg1)
			{
				return string.CompareOrdinal(Sharpen.Runtime.getClassForObject(arg0).getName(), Sharpen.Runtime.getClassForObject
					(arg1).getName());
			}
		}

		private sealed class _Comparator_126 : java.util.Comparator<org.apache.hadoop.fs.shell.find.Expression
			>
		{
			public _Comparator_126()
			{
			}

			public int compare(org.apache.hadoop.fs.shell.find.Expression arg0, org.apache.hadoop.fs.shell.find.Expression
				 arg1)
			{
				return string.CompareOrdinal(Sharpen.Runtime.getClassForObject(arg0).getName(), Sharpen.Runtime.getClassForObject
					(arg1).getName());
			}
		}

		/// <summary>Default constructor for the Find command.</summary>
		public Find()
		{
			setRecursive(true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(1, int.MaxValue, OPTION_FOLLOW_LINK, OPTION_FOLLOW_ARG_LINK, null);
			cf.parse(args);
			if (cf.getOpt(OPTION_FOLLOW_LINK))
			{
				getOptions().setFollowLink(true);
			}
			else
			{
				if (cf.getOpt(OPTION_FOLLOW_ARG_LINK))
				{
					getOptions().setFollowArgLink(true);
				}
			}
			// search for first non-path argument (ie starts with a "-") and capture and
			// remove the remaining arguments as expressions
			System.Collections.Generic.LinkedList<string> expressionArgs = new System.Collections.Generic.LinkedList
				<string>();
			System.Collections.Generic.IEnumerator<string> it = args.GetEnumerator();
			bool isPath = true;
			while (it.MoveNext())
			{
				string arg = it.Current;
				if (isPath)
				{
					if (arg.StartsWith("-"))
					{
						isPath = false;
					}
				}
				if (!isPath)
				{
					expressionArgs.add(arg);
					it.remove();
				}
			}
			if (args.isEmpty())
			{
				args.add(org.apache.hadoop.fs.Path.CUR_DIR);
			}
			org.apache.hadoop.fs.shell.find.Expression expression = parseExpression(expressionArgs
				);
			if (!expression.isAction())
			{
				org.apache.hadoop.fs.shell.find.Expression and = getExpression(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.fs.shell.find.And)));
				java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> children = new System.Collections.Generic.LinkedList
					<org.apache.hadoop.fs.shell.find.Expression>();
				children.add(getExpression(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Print
					))));
				children.add(expression);
				and.addChildren(children);
				expression = and;
			}
			setRootExpression(expression);
		}

		/// <summary>Set the root expression for this find.</summary>
		/// <param name="expression"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		internal virtual void setRootExpression(org.apache.hadoop.fs.shell.find.Expression
			 expression)
		{
			this.rootExpression = expression;
		}

		/// <summary>Return the root expression for this find.</summary>
		/// <returns>the root expression</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		internal virtual org.apache.hadoop.fs.shell.find.Expression getRootExpression()
		{
			return this.rootExpression;
		}

		/// <summary>Returns the current find options, creating them if necessary.</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		internal virtual org.apache.hadoop.fs.shell.find.FindOptions getOptions()
		{
			if (options == null)
			{
				options = createOptions();
			}
			return options;
		}

		/// <summary>Create a new set of find options.</summary>
		private org.apache.hadoop.fs.shell.find.FindOptions createOptions()
		{
			org.apache.hadoop.fs.shell.find.FindOptions options = new org.apache.hadoop.fs.shell.find.FindOptions
				();
			options.setOut(@out);
			options.setErr(err);
			options.setIn(Sharpen.Runtime.@in);
			options.setCommandFactory(getCommandFactory());
			options.setConfiguration(getConf());
			return options;
		}

		/// <summary>
		/// Add the
		/// <see cref="org.apache.hadoop.fs.shell.PathData"/>
		/// item to the stop set.
		/// </summary>
		private void addStop(org.apache.hadoop.fs.shell.PathData item)
		{
			stopPaths.add(item.path);
		}

		/// <summary>
		/// Returns true if the
		/// <see cref="org.apache.hadoop.fs.shell.PathData"/>
		/// item is in the stop set.
		/// </summary>
		private bool isStop(org.apache.hadoop.fs.shell.PathData item)
		{
			return stopPaths.contains(item.path);
		}

		/// <summary>
		/// Parse a list of arguments to to extract the
		/// <see cref="Expression"/>
		/// elements.
		/// The input Deque will be modified to remove the used elements.
		/// </summary>
		/// <param name="args">arguments to be parsed</param>
		/// <returns>
		/// list of
		/// <see cref="Expression"/>
		/// elements applicable to this command
		/// </returns>
		/// <exception cref="System.IO.IOException">if list can not be parsed</exception>
		private org.apache.hadoop.fs.shell.find.Expression parseExpression(java.util.Deque
			<string> args)
		{
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> primaries = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			java.util.Deque<org.apache.hadoop.fs.shell.find.Expression> operators = new System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.find.Expression>();
			org.apache.hadoop.fs.shell.find.Expression prevExpr = getExpression(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.shell.find.And)));
			while (!args.isEmpty())
			{
				string arg = args.pop();
				if ("(".Equals(arg))
				{
					org.apache.hadoop.fs.shell.find.Expression expr = parseExpression(args);
					primaries.add(expr);
					prevExpr = new _BaseExpression_281();
				}
				else
				{
					// stub the previous expression to be a non-op
					if (")".Equals(arg))
					{
						break;
					}
					else
					{
						if (isExpression(arg))
						{
							org.apache.hadoop.fs.shell.find.Expression expr = getExpression(arg);
							expr.addArguments(args);
							if (expr.isOperator())
							{
								while (!operators.isEmpty())
								{
									if (operators.peek().getPrecedence() >= expr.getPrecedence())
									{
										org.apache.hadoop.fs.shell.find.Expression op = operators.pop();
										op.addChildren(primaries);
										primaries.push(op);
									}
									else
									{
										break;
									}
								}
								operators.push(expr);
							}
							else
							{
								if (!prevExpr.isOperator())
								{
									org.apache.hadoop.fs.shell.find.Expression and = getExpression(Sharpen.Runtime.getClassForType
										(typeof(org.apache.hadoop.fs.shell.find.And)));
									while (!operators.isEmpty())
									{
										if (operators.peek().getPrecedence() >= and.getPrecedence())
										{
											org.apache.hadoop.fs.shell.find.Expression op = operators.pop();
											op.addChildren(primaries);
											primaries.push(op);
										}
										else
										{
											break;
										}
									}
									operators.push(and);
								}
								primaries.push(expr);
							}
							prevExpr = expr;
						}
						else
						{
							throw new System.IO.IOException("Unexpected argument: " + arg);
						}
					}
				}
			}
			while (!operators.isEmpty())
			{
				org.apache.hadoop.fs.shell.find.Expression @operator = operators.pop();
				@operator.addChildren(primaries);
				primaries.push(@operator);
			}
			return primaries.isEmpty() ? getExpression(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.fs.shell.find.Print))) : primaries.pop();
		}

		private sealed class _BaseExpression_281 : org.apache.hadoop.fs.shell.find.BaseExpression
		{
			public _BaseExpression_281()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
				 item, int depth)
			{
				return org.apache.hadoop.fs.shell.find.Result.PASS;
			}
		}

		/// <summary>Returns true if the target is an ancestor of the source.</summary>
		private bool isAncestor(org.apache.hadoop.fs.shell.PathData source, org.apache.hadoop.fs.shell.PathData
			 target)
		{
			for (org.apache.hadoop.fs.Path parent = source.path; (parent != null) && !parent.
				isRoot(); parent = parent.getParent())
			{
				if (parent.Equals(target.path))
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void recursePath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			if (isStop(item))
			{
				// this item returned a stop result so don't recurse any further
				return;
			}
			if (getDepth() >= getOptions().getMaxDepth())
			{
				// reached the maximum depth so don't got any further.
				return;
			}
			if (item.stat.isSymlink() && getOptions().isFollowLink())
			{
				org.apache.hadoop.fs.shell.PathData linkedItem = new org.apache.hadoop.fs.shell.PathData
					(item.stat.getSymlink().ToString(), getConf());
				if (isAncestor(item, linkedItem))
				{
					getOptions().getErr().WriteLine("Infinite loop ignored: " + item.ToString() + " -> "
						 + linkedItem.ToString());
					return;
				}
				if (linkedItem.exists)
				{
					item = linkedItem;
				}
			}
			if (item.stat.isDirectory())
			{
				base.recursePath(item);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool isPathRecursable(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			if (item.stat.isDirectory())
			{
				return true;
			}
			if (item.stat.isSymlink())
			{
				org.apache.hadoop.fs.shell.PathData linkedItem = new org.apache.hadoop.fs.shell.PathData
					(item.fs.resolvePath(item.stat.getSymlink()).ToString(), getConf());
				if (linkedItem.stat.isDirectory())
				{
					if (getOptions().isFollowLink())
					{
						return true;
					}
					if (getOptions().isFollowArgLink() && (getDepth() == 0))
					{
						return true;
					}
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			if (getOptions().isDepthFirst())
			{
				// depth first so leave until post processing
				return;
			}
			applyItem(item);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void postProcessPath(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			if (!getOptions().isDepthFirst())
			{
				// not depth first so already processed
				return;
			}
			applyItem(item);
		}

		/// <exception cref="System.IO.IOException"/>
		private void applyItem(org.apache.hadoop.fs.shell.PathData item)
		{
			if (getDepth() >= getOptions().getMinDepth())
			{
				org.apache.hadoop.fs.shell.find.Result result = getRootExpression().apply(item, getDepth
					());
				if (org.apache.hadoop.fs.shell.find.Result.STOP.Equals(result))
				{
					addStop(item);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processArguments(System.Collections.Generic.LinkedList
			<org.apache.hadoop.fs.shell.PathData> args)
		{
			org.apache.hadoop.fs.shell.find.Expression expr = getRootExpression();
			expr.setOptions(getOptions());
			expr.prepare();
			base.processArguments(args);
			expr.finish();
		}

		/// <summary>Gets a named expression from the factory.</summary>
		private org.apache.hadoop.fs.shell.find.Expression getExpression(string expressionName
			)
		{
			return org.apache.hadoop.fs.shell.find.ExpressionFactory.getExpressionFactory().getExpression
				(expressionName, getConf());
		}

		/// <summary>Gets an instance of an expression from the factory.</summary>
		private org.apache.hadoop.fs.shell.find.Expression getExpression(java.lang.Class 
			expressionClass)
		{
			return org.apache.hadoop.fs.shell.find.ExpressionFactory.getExpressionFactory().createExpression
				(expressionClass, getConf());
		}

		/// <summary>Asks the factory whether an expression is recognized.</summary>
		private bool isExpression(string expressionName)
		{
			return org.apache.hadoop.fs.shell.find.ExpressionFactory.getExpressionFactory().isExpression
				(expressionName);
		}
	}
}
