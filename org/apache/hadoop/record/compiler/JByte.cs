using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Code generator for "byte" type.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JByte : org.apache.hadoop.record.compiler.JType
	{
		internal class JavaByte : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaByte(JByte _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.ByteTypeID";
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("if (" + l + "<1) {\n");
				cb.append("throw new java.io.IOException(\"Byte is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append(s + "++; " + l + "--;\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("if (l1<1 || l2<1) {\n");
				cb.append("throw new java.io.IOException(\"Byte is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append("if (b1[s1] != b2[s2]) {\n");
				cb.append("return (b1[s1]<b2[s2])?-1:0;\n");
				cb.append("}\n");
				cb.append("s1++; s2++; l1--; l2--;\n");
				cb.append("}\n");
			}

			private readonly JByte _enclosing;
		}

		internal class CppByte : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppByte(JByte _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BYTE)";
			}

			private readonly JByte _enclosing;
		}

		public JByte()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JByte.JavaByte(this));
			setCppType(new org.apache.hadoop.record.compiler.JByte.CppByte(this));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
		}

		internal override string getSignature()
		{
			return "b";
		}
	}
}
