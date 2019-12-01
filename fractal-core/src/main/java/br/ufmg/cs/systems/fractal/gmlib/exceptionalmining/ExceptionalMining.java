package br.ufmg.cs.systems.fractal.gmlib.exceptionalmining;

import br.ufmg.cs.systems.fractal.graph.BasicMainGraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.collect.set.hash.HashObjSets;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class ExceptionalMining
      extends BasicMainGraph<IntArrayList, IntArrayList> {
   public ExceptionalMining(String name) {
      super(name, false, false);
   }

   public ExceptionalMining(String name, boolean isEdgeLabelled,
                            boolean isMultiGraph) {
      super(name, isEdgeLabelled, isMultiGraph);
   }

   public ExceptionalMining(Path filePath, boolean isEdgeLabelled,
                            boolean isMultiGraph) throws IOException {
      super(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
   }

   public ExceptionalMining(org.apache.hadoop.fs.Path hdfsPath,
                            boolean isEdgeLabelled, boolean isMultiGraph) throws IOException {
      super(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
   }

   private IntArrayList parseWordSet(StringTokenizer tokenizer) {
      IntArrayList prop = new IntArrayList();
      Double j;
      while (tokenizer.hasMoreTokens()) {
          j = Double.parseDouble(tokenizer.nextToken());
         prop.add(j.intValue());
      }

      return prop;
   }
   
   @Override
   protected IntArrayList parseVertexProperty(StringTokenizer tokenizer) {
      return parseWordSet(tokenizer);
   }
   
   @Override
   protected IntArrayList parseEdgeProperty(StringTokenizer tokenizer) {
      return parseWordSet(tokenizer);
   }

}
