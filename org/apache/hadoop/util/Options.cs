using Sharpen;

namespace org.apache.hadoop.util
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

			public virtual string getValue()
			{
				return value;
			}
		}

		public abstract class ClassOption
		{
			private readonly java.lang.Class value;

			protected internal ClassOption(java.lang.Class value)
			{
				this.value = value;
			}

			public virtual java.lang.Class getValue()
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

			public virtual bool getValue()
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

			public virtual int getValue()
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

			public virtual long getValue()
			{
				return value;
			}
		}

		public abstract class PathOption
		{
			private readonly org.apache.hadoop.fs.Path value;

			protected internal PathOption(org.apache.hadoop.fs.Path value)
			{
				this.value = value;
			}

			public virtual org.apache.hadoop.fs.Path getValue()
			{
				return value;
			}
		}

		public abstract class FSDataInputStreamOption
		{
			private readonly org.apache.hadoop.fs.FSDataInputStream value;

			protected internal FSDataInputStreamOption(org.apache.hadoop.fs.FSDataInputStream
				 value)
			{
				this.value = value;
			}

			public virtual org.apache.hadoop.fs.FSDataInputStream getValue()
			{
				return value;
			}
		}

		public abstract class FSDataOutputStreamOption
		{
			private readonly org.apache.hadoop.fs.FSDataOutputStream value;

			protected internal FSDataOutputStreamOption(org.apache.hadoop.fs.FSDataOutputStream
				 value)
			{
				this.value = value;
			}

			public virtual org.apache.hadoop.fs.FSDataOutputStream getValue()
			{
				return value;
			}
		}

		public abstract class ProgressableOption
		{
			private readonly org.apache.hadoop.util.Progressable value;

			protected internal ProgressableOption(org.apache.hadoop.util.Progressable value)
			{
				this.value = value;
			}

			public virtual org.apache.hadoop.util.Progressable getValue()
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
		public static T getOption<@base, T>(@base[] opts)
			where T : @base
		{
			System.Type cls = typeof(T);
			foreach (@base o in opts)
			{
				if (Sharpen.Runtime.getClassForObject(o) == cls)
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
		public static T[] prependOptions<T>(T[] oldOpts, params T[] newOpts)
		{
			// copy the new options to the front of the array
			T[] result = java.util.Arrays.copyOf(newOpts, newOpts.Length + oldOpts.Length);
			// now copy the old options
			System.Array.Copy(oldOpts, 0, result, newOpts.Length, oldOpts.Length);
			return result;
		}
	}
}
