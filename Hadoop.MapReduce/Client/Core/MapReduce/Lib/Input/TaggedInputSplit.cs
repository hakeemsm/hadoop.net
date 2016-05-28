using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputSplit"/>
	/// that tags another InputSplit with extra data for use
	/// by
	/// <see cref="DelegatingInputFormat{K, V}"/>
	/// s and
	/// <see cref="DelegatingMapper{K1, V1, K2, V2}"/>
	/// s.
	/// </summary>
	internal class TaggedInputSplit : InputSplit, Configurable, Writable
	{
		private Type inputSplitClass;

		private InputSplit inputSplit;

		private Type inputFormatClass;

		private Type mapperClass;

		private Configuration conf;

		public TaggedInputSplit()
		{
		}

		/// <summary>Creates a new TaggedInputSplit.</summary>
		/// <param name="inputSplit">The InputSplit to be tagged</param>
		/// <param name="conf">The configuration to use</param>
		/// <param name="inputFormatClass">The InputFormat class to use for this job</param>
		/// <param name="mapperClass">The Mapper class to use for this job</param>
		public TaggedInputSplit(InputSplit inputSplit, Configuration conf, Type inputFormatClass
			, Type mapperClass)
		{
			// Default constructor.
			this.inputSplitClass = inputSplit.GetType();
			this.inputSplit = inputSplit;
			this.conf = conf;
			this.inputFormatClass = inputFormatClass;
			this.mapperClass = mapperClass;
		}

		/// <summary>Retrieves the original InputSplit.</summary>
		/// <returns>The InputSplit that was tagged</returns>
		public virtual InputSplit GetInputSplit()
		{
			return inputSplit;
		}

		/// <summary>Retrieves the InputFormat class to use for this split.</summary>
		/// <returns>The InputFormat class to use</returns>
		public virtual Type GetInputFormatClass()
		{
			return inputFormatClass;
		}

		/// <summary>Retrieves the Mapper class to use for this split.</summary>
		/// <returns>The Mapper class to use</returns>
		public virtual Type GetMapperClass()
		{
			return mapperClass;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long GetLength()
		{
			return inputSplit.GetLength();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string[] GetLocations()
		{
			return inputSplit.GetLocations();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			inputSplitClass = (Type)ReadClass(@in);
			inputFormatClass = (Type)ReadClass(@in);
			mapperClass = (Type)ReadClass(@in);
			inputSplit = (InputSplit)ReflectionUtils.NewInstance(inputSplitClass, conf);
			SerializationFactory factory = new SerializationFactory(conf);
			Deserializer deserializer = factory.GetDeserializer(inputSplitClass);
			deserializer.Open((DataInputStream)@in);
			inputSplit = (InputSplit)deserializer.Deserialize(inputSplit);
		}

		/// <exception cref="System.IO.IOException"/>
		private Type ReadClass(DataInput @in)
		{
			string className = StringInterner.WeakIntern(Text.ReadString(@in));
			try
			{
				return conf.GetClassByName(className);
			}
			catch (TypeLoadException e)
			{
				throw new RuntimeException("readObject can't find class", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, inputSplitClass.FullName);
			Text.WriteString(@out, inputFormatClass.FullName);
			Text.WriteString(@out, mapperClass.FullName);
			SerializationFactory factory = new SerializationFactory(conf);
			Org.Apache.Hadoop.IO.Serializer.Serializer serializer = factory.GetSerializer(inputSplitClass
				);
			serializer.Open((DataOutputStream)@out);
			serializer.Serialize(inputSplit);
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public override string ToString()
		{
			return inputSplit.ToString();
		}
	}
}
