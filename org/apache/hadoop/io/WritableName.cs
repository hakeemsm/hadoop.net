using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// Utility to permit renaming of Writable implementation classes without
	/// invalidiating files that contain their class name.
	/// </summary>
	public class WritableName
	{
		private static System.Collections.Generic.Dictionary<string, java.lang.Class> NAME_TO_CLASS
			 = new System.Collections.Generic.Dictionary<string, java.lang.Class>();

		private static System.Collections.Generic.Dictionary<java.lang.Class, string> CLASS_TO_NAME
			 = new System.Collections.Generic.Dictionary<java.lang.Class, string>();

		static WritableName()
		{
			// define important types
			org.apache.hadoop.io.WritableName.setName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.NullWritable)), "null");
			org.apache.hadoop.io.WritableName.setName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.LongWritable)), "long");
			org.apache.hadoop.io.WritableName.setName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.UTF8)), "UTF8");
			org.apache.hadoop.io.WritableName.setName(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.MD5Hash)), "MD5Hash");
		}

		private WritableName()
		{
		}

		// no public ctor
		/// <summary>
		/// Set the name that a class should be known as to something other than the
		/// class name.
		/// </summary>
		public static void setName(java.lang.Class writableClass, string name)
		{
			lock (typeof(WritableName))
			{
				CLASS_TO_NAME[writableClass] = name;
				NAME_TO_CLASS[name] = writableClass;
			}
		}

		/// <summary>Add an alternate name for a class.</summary>
		public static void addName(java.lang.Class writableClass, string name)
		{
			lock (typeof(WritableName))
			{
				NAME_TO_CLASS[name] = writableClass;
			}
		}

		/// <summary>Return the name for a class.</summary>
		/// <remarks>
		/// Return the name for a class.  Default is
		/// <see cref="java.lang.Class{T}.getName()"/>
		/// .
		/// </remarks>
		public static string getName(java.lang.Class writableClass)
		{
			lock (typeof(WritableName))
			{
				string name = CLASS_TO_NAME[writableClass];
				if (name != null)
				{
					return name;
				}
				return writableClass.getName();
			}
		}

		/// <summary>Return the class for a name.</summary>
		/// <remarks>
		/// Return the class for a name.  Default is
		/// <see cref="java.lang.Class{T}.forName(string)"/>
		/// .
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static java.lang.Class getClass(string name, org.apache.hadoop.conf.Configuration
			 conf)
		{
			lock (typeof(WritableName))
			{
				java.lang.Class writableClass = NAME_TO_CLASS[name];
				if (writableClass != null)
				{
					return writableClass.asSubclass<org.apache.hadoop.io.Writable>();
				}
				try
				{
					return conf.getClassByName(name);
				}
				catch (java.lang.ClassNotFoundException e)
				{
					System.IO.IOException newE = new System.IO.IOException("WritableName can't load class: "
						 + name);
					newE.initCause(e);
					throw newE;
				}
			}
		}
	}
}
