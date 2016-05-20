using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A wrapper for Writable instances.</summary>
	/// <remarks>
	/// A wrapper for Writable instances.
	/// <p>
	/// When two sequence files, which have same Key type but different Value
	/// types, are mapped out to reduce, multiple Value types is not allowed.
	/// In this case, this class can help you wrap instances with different types.
	/// </p>
	/// <p>
	/// Compared with <code>ObjectWritable</code>, this class is much more effective,
	/// because <code>ObjectWritable</code> will append the class declaration as a String
	/// into the output file in every Key-Value pair.
	/// </p>
	/// <p>
	/// Generic Writable implements
	/// <see cref="org.apache.hadoop.conf.Configurable"/>
	/// interface, so that it will be
	/// configured by the framework. The configuration is passed to the wrapped objects
	/// implementing
	/// <see cref="org.apache.hadoop.conf.Configurable"/>
	/// interface <i>before deserialization</i>.
	/// </p>
	/// how to use it: <br />
	/// 1. Write your own class, such as GenericObject, which extends GenericWritable.<br />
	/// 2. Implements the abstract method <code>getTypes()</code>, defines
	/// the classes which will be wrapped in GenericObject in application.
	/// Attention: this classes defined in <code>getTypes()</code> method, must
	/// implement <code>Writable</code> interface.
	/// <br /><br />
	/// The code looks like this:
	/// <blockquote><pre>
	/// public class GenericObject extends GenericWritable {
	/// private static Class[] CLASSES = {
	/// ClassType1.class,
	/// ClassType2.class,
	/// ClassType3.class,
	/// };
	/// protected Class[] getTypes() {
	/// return CLASSES;
	/// }
	/// }
	/// </pre></blockquote>
	/// </remarks>
	/// <since>Nov 8, 2006</since>
	public abstract class GenericWritable : org.apache.hadoop.io.Writable, org.apache.hadoop.conf.Configurable
	{
		private const byte NOT_SET = unchecked((byte)(-1));

		private byte type = NOT_SET;

		private org.apache.hadoop.io.Writable instance;

		private org.apache.hadoop.conf.Configuration conf = null;

		/// <summary>Set the instance that is wrapped.</summary>
		/// <param name="obj"/>
		public virtual void set(org.apache.hadoop.io.Writable obj)
		{
			instance = obj;
			java.lang.Class instanceClazz = Sharpen.Runtime.getClassForObject(instance);
			java.lang.Class[] clazzes = getTypes();
			for (int i = 0; i < clazzes.Length; i++)
			{
				java.lang.Class clazz = clazzes[i];
				if (clazz.Equals(instanceClazz))
				{
					type = unchecked((byte)i);
					return;
				}
			}
			throw new System.Exception("The type of instance is: " + Sharpen.Runtime.getClassForObject
				(instance) + ", which is NOT registered.");
		}

		/// <summary>Return the wrapped instance.</summary>
		public virtual org.apache.hadoop.io.Writable get()
		{
			return instance;
		}

		public override string ToString()
		{
			return "GW[" + (instance != null ? ("class=" + Sharpen.Runtime.getClassForObject(
				instance).getName() + ",value=" + instance.ToString()) : "(null)") + "]";
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			type = @in.readByte();
			java.lang.Class clazz = getTypes()[type & unchecked((int)(0xff))];
			try
			{
				instance = org.apache.hadoop.util.ReflectionUtils.newInstance(clazz, conf);
			}
			catch (System.Exception e)
			{
				Sharpen.Runtime.printStackTrace(e);
				throw new System.IO.IOException("Cannot initialize the class: " + clazz);
			}
			instance.readFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			if (type == NOT_SET || instance == null)
			{
				throw new System.IO.IOException("The GenericWritable has NOT been set correctly. type="
					 + type + ", instance=" + instance);
			}
			@out.writeByte(type);
			instance.write(@out);
		}

		/// <summary>Return all classes that may be wrapped.</summary>
		/// <remarks>
		/// Return all classes that may be wrapped.  Subclasses should implement this
		/// to return a constant array of classes.
		/// </remarks>
		protected internal abstract java.lang.Class[] getTypes();

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}
	}
}
