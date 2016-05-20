using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Code generator for "long" type</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JLong : org.apache.hadoop.record.compiler.JType
	{
		internal class JavaLong : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaLong(JLong _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.LongTypeID";
			}

			internal override void genHashCode(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (int) (" +
					 fname + "^(" + fname + ">>>32));\n");
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("long i = org.apache.hadoop.record.Utils.readVLong(" + b + ", " + s + ");\n"
					);
				cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.append(s + "+=z; " + l + "-=z;\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);\n");
				cb.append("long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);\n");
				cb.append("if (i1 != i2) {\n");
				cb.append("return ((i1-i2) < 0) ? -1 : 0;\n");
				cb.append("}\n");
				cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.append("}\n");
			}

			private readonly JLong _enclosing;
		}

		internal class CppLong : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppLong(JLong _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_LONG)";
			}

			private readonly JLong _enclosing;
		}

		/// <summary>Creates a new instance of JLong</summary>
		public JLong()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JLong.JavaLong(this));
			setCppType(new org.apache.hadoop.record.compiler.JLong.CppLong(this));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
		}

		internal override string getSignature()
		{
			return "l";
		}
	}
}
