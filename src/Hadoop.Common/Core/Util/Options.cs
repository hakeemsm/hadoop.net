using System;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// This class allows generic access to variable length type-safe parameter
	/// lists.
	/// </summary>
	public class Options
	{
		public abstract class StringOption
		{
			private readonly string value;

			protected internal StringOption(string value)
			{
				this.value = value;
			}

			public virtual string GetValue()
			{
				return value;
			}
		}

		public abstract class ClassOption
		{
			private readonly Type value;

			protected internal ClassOption(Type value)
			{
				this.value = value;
			}

			public virtual Type GetValue()
			{
				return value;
			}
		}

		public abstract class BooleanOption
		{
			private readonly bool value;

			protected internal BooleanOption(bool value)
			{
				this.value = value;
			}

			public virtual bool GetValue()
			{
				return value;
			}
		}

		public abstract class IntegerOption
		{
			private readonly int value;

			protected internal IntegerOption(int value)
			{
				this.value = value;
			}

			public virtual int GetValue()
			{
				return value;
			}
		}

		public abstract class LongOption
		{
			private readonly long value;

			protected internal LongOption(long value)
			{
				this.value = value;
			}

			public virtual long GetValue()
			{
				return value;
			}
		}

		public abstract class PathOption
		{
			private readonly Path value;

			protected internal PathOption(Path value)
			{
				this.value = value;
			}

			public virtual Path GetValue()
			{
				return value;
			}
		}

		public abstract class FSDataInputStreamOption
		{
			private readonly FSDataInputStream value;

			protected internal FSDataInputStreamOption(FSDataInputStream value)
			{
				this.value = value;
			}

			public virtual FSDataInputStream GetValue()
			{
				return value;
			}
		}

		public abstract class FSDataOutputStreamOption
		{
			private readonly FSDataOutputStream value;

			protected internal FSDataOutputStreamOption(FSDataOutputStream value)
			{
				this.value = value;
			}

			public virtual FSDataOutputStream GetValue()
			{
				return value;
			}
		}

		public abstract class ProgressableOption
		{
			private readonly Progressable value;

			protected internal ProgressableOption(Progressable value)
			{
				this.value = value;
			}

			public virtual Progressable GetValue()
			{
				return value;
			}
		}

		/// <summary>Find the first option of the required class.</summary>
		/// <?/>
		/// <?/>
		/// <param name="cls">the dynamic class to find</param>
		/// <param name="opts">the list of options to look through</param>
		/// <returns>the first option that matches</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T GetOption<@base, T>(Base[] opts)
			where T : Base
		{
			System.Type cls = typeof(T);
			foreach (Base o in opts)
			{
				if (o.GetType() == cls)
				{
					return (T)o;
				}
			}
			return null;
		}

		/// <summary>Prepend some new options to the old options</summary>
		/// <?/>
		/// <param name="oldOpts">the old options</param>
		/// <param name="newOpts">the new options</param>
		/// <returns>a new array of options</returns>
		public static T[] PrependOptions<T>(T[] oldOpts, params T[] newOpts)
		{
			// copy the new options to the front of the array
			T[] result = Arrays.CopyOf(newOpts, newOpts.Length + oldOpts.Length);
			// now copy the old options
			System.Array.Copy(oldOpts, 0, result, newOpts.Length, oldOpts.Length);
			return result;
		}
	}
}
