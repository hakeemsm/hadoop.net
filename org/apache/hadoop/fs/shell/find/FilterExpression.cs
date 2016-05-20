using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Provides an abstract composition filter for the
	/// <see cref="Expression"/>
	/// interface.
	/// Allows other
	/// <see cref="Expression"/>
	/// implementations to be reused without
	/// inheritance.
	/// </summary>
	public abstract class FilterExpression : org.apache.hadoop.fs.shell.find.Expression
		, org.apache.hadoop.conf.Configurable
	{
		protected internal org.apache.hadoop.fs.shell.find.Expression expression;

		protected internal FilterExpression(org.apache.hadoop.fs.shell.find.Expression expression
			)
		{
			this.expression = expression;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void setOptions(org.apache.hadoop.fs.shell.find.FindOptions options
			)
		{
			if (expression != null)
			{
				expression.setOptions(options);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void prepare()
		{
			if (expression != null)
			{
				expression.prepare();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
			 item, int depth)
		{
			if (expression != null)
			{
				return expression.apply(item, -1);
			}
			return org.apache.hadoop.fs.shell.find.Result.PASS;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void finish()
		{
			if (expression != null)
			{
				expression.finish();
			}
		}

		public virtual string[] getUsage()
		{
			if (expression != null)
			{
				return expression.getUsage();
			}
			return null;
		}

		public virtual string[] getHelp()
		{
			if (expression != null)
			{
				return expression.getHelp();
			}
			return null;
		}

		public virtual bool isAction()
		{
			if (expression != null)
			{
				return expression.isAction();
			}
			return false;
		}

		public virtual bool isOperator()
		{
			if (expression != null)
			{
				return expression.isOperator();
			}
			return false;
		}

		public virtual int getPrecedence()
		{
			if (expression != null)
			{
				return expression.getPrecedence();
			}
			return -1;
		}

		public virtual void addChildren(java.util.Deque<org.apache.hadoop.fs.shell.find.Expression
			> expressions)
		{
			if (expression != null)
			{
				expression.addChildren(expressions);
			}
		}

		public virtual void addArguments(java.util.Deque<string> args)
		{
			if (expression != null)
			{
				expression.addArguments(args);
			}
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			if (expression is org.apache.hadoop.conf.Configurable)
			{
				((org.apache.hadoop.conf.Configurable)expression).setConf(conf);
			}
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			if (expression is org.apache.hadoop.conf.Configurable)
			{
				return ((org.apache.hadoop.conf.Configurable)expression).getConf();
			}
			return null;
		}

		public override string ToString()
		{
			if (expression != null)
			{
				return Sharpen.Runtime.getClassForObject(this).getSimpleName() + "-" + expression
					.ToString();
			}
			return Sharpen.Runtime.getClassForObject(this).getSimpleName();
		}
	}
}
