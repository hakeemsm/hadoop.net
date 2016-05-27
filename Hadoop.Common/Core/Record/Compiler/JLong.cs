using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>Code generator for "long" type</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JLong : JType
	{
		internal class JavaLong : JType.JavaType
		{
			internal JavaLong(JLong _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.LongTypeID";
			}

			internal override void GenHashCode(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "ret = (int) (" + fname + "^(" + fname + ">>>32));\n"
					);
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("long i = org.apache.hadoop.record.Utils.readVLong(" + b + ", " + s + ");\n"
					);
				cb.Append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.Append(s + "+=z; " + l + "-=z;\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("long i1 = org.apache.hadoop.record.Utils.readVLong(b1, s1);\n");
				cb.Append("long i2 = org.apache.hadoop.record.Utils.readVLong(b2, s2);\n");
				cb.Append("if (i1 != i2) {\n");
				cb.Append("return ((i1-i2) < 0) ? -1 : 0;\n");
				cb.Append("}\n");
				cb.Append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.Append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.Append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.Append("}\n");
			}

			private readonly JLong _enclosing;
		}

		internal class CppLong : JType.CppType
		{
			internal CppLong(JLong _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_LONG)";
			}

			private readonly JLong _enclosing;
		}

		/// <summary>Creates a new instance of JLong</summary>
		public JLong()
		{
			SetJavaType(new JLong.JavaLong(this));
			SetCppType(new JLong.CppLong(this));
			SetCType(new JType.CType(this));
		}

		internal override string GetSignature()
		{
			return "l";
		}
	}
}
