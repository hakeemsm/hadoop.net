using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>Code generator for "buffer" type.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JBuffer : JCompType
	{
		internal class JavaBuffer : JCompType.JavaCompType
		{
			internal JavaBuffer(JBuffer _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.BufferTypeID";
			}

			internal override void GenCompareTo(CodeBuffer cb, string fname, string other)
			{
				cb.Append(Consts.RioPrefix + "ret = " + fname + ".compareTo(" + other + ");\n");
			}

			internal override void GenEquals(CodeBuffer cb, string fname, string peer)
			{
				cb.Append(Consts.RioPrefix + "ret = " + fname + ".equals(" + peer + ");\n");
			}

			internal override void GenHashCode(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "ret = " + fname + ".hashCode();\n");
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("int i = org.apache.hadoop.record.Utils.readVInt(" + b + ", " + s + ");\n"
					);
				cb.Append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.Append(s + " += z+i; " + l + " -= (z+i);\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
				cb.Append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
				cb.Append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.Append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.Append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.Append("int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);\n"
					);
				cb.Append("if (r1 != 0) { return (r1<0)?-1:0; }\n");
				cb.Append("s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
				cb.Append("}\n");
			}

			private readonly JBuffer _enclosing;
		}

		internal class CppBuffer : JCompType.CppCompType
		{
			internal CppBuffer(JBuffer _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void GenGetSet(CodeBuffer cb, string fname)
			{
				cb.Append("virtual const " + this.GetType() + "& get" + JType.ToCamelCase(fname) 
					+ "() const {\n");
				cb.Append("return " + fname + ";\n");
				cb.Append("}\n");
				cb.Append("virtual " + this.GetType() + "& get" + JType.ToCamelCase(fname) + "() {\n"
					);
				cb.Append("return " + fname + ";\n");
				cb.Append("}\n");
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BUFFER)";
			}

			private readonly JBuffer _enclosing;
		}

		/// <summary>Creates a new instance of JBuffer</summary>
		public JBuffer()
		{
			SetJavaType(new JBuffer.JavaBuffer(this));
			SetCppType(new JBuffer.CppBuffer(this));
			SetCType(new JCompType.CCompType(this));
		}

		internal override string GetSignature()
		{
			return "B";
		}
	}
}
