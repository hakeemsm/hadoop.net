using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
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
	public abstract class FilterExpression : Expression, Configurable
	{
		protected internal Expression expression;

		protected internal FilterExpression(Expression expression)
		{
			this.expression = expression;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetOptions(FindOptions options)
		{
			if (expression != null)
			{
				expression.SetOptions(options);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Prepare()
		{
			if (expression != null)
			{
				expression.Prepare();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Result Apply(PathData item, int depth)
		{
			if (expression != null)
			{
				return expression.Apply(item, -1);
			}
			return Result.Pass;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Finish()
		{
			if (expression != null)
			{
				expression.Finish();
			}
		}

		public virtual string[] GetUsage()
		{
			if (expression != null)
			{
				return expression.GetUsage();
			}
			return null;
		}

		public virtual string[] GetHelp()
		{
			if (expression != null)
			{
				return expression.GetHelp();
			}
			return null;
		}

		public virtual bool IsAction()
		{
			if (expression != null)
			{
				return expression.IsAction();
			}
			return false;
		}

		public virtual bool IsOperator()
		{
			if (expression != null)
			{
				return expression.IsOperator();
			}
			return false;
		}

		public virtual int GetPrecedence()
		{
			if (expression != null)
			{
				return expression.GetPrecedence();
			}
			return -1;
		}

		public virtual void AddChildren(Deque<Expression> expressions)
		{
			if (expression != null)
			{
				expression.AddChildren(expressions);
			}
		}

		public virtual void AddArguments(Deque<string> args)
		{
			if (expression != null)
			{
				expression.AddArguments(args);
			}
		}

		public virtual void SetConf(Configuration conf)
		{
			if (expression is Configurable)
			{
				((Configurable)expression).SetConf(conf);
			}
		}

		public virtual Configuration GetConf()
		{
			if (expression is Configurable)
			{
				return ((Configurable)expression).GetConf();
			}
			return null;
		}

		public override string ToString()
		{
			if (expression != null)
			{
				return GetType().Name + "-" + expression.ToString();
			}
			return GetType().Name;
		}
	}
}
