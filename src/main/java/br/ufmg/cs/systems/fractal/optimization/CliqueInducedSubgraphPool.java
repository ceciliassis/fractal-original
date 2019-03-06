package br.ufmg.cs.systems.fractal.optimization;

import br.ufmg.cs.systems.fractal.util.BasicFactory;
import br.ufmg.cs.systems.fractal.util.Factory;
import br.ufmg.cs.systems.fractal.util.pool.Pool;

public class CliqueInducedSubgraphPool extends Pool<CliqueInducedSubgraph> {

    public static CliqueInducedSubgraphPool instance() {
       return CliqueInducedSubgraphPoolHolder.INSTANCE;
    }
    
    public CliqueInducedSubgraphPool(Factory<CliqueInducedSubgraph> factory) {
        super(factory);
    }

    private static class CliqueInducedSubgraphFactory extends BasicFactory<CliqueInducedSubgraph> {
        @Override
        public CliqueInducedSubgraph createObject() {
            return new CliqueInducedSubgraph();
        }
    }

    private class CliqueInducedSubgraphReclaimer extends ObjReclaimer {
       @Override
       public void accept(CliqueInducedSubgraph o) {
          o.clear();
          super.accept(o);
       }
    }

    @Override
    protected ObjReclaimer createObjReclaimer() {
       return new CliqueInducedSubgraphReclaimer();
    }

    /*
     * Delayed creation, instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class CliqueInducedSubgraphPoolHolder {
        static final CliqueInducedSubgraphPool INSTANCE =
           new CliqueInducedSubgraphPool(new CliqueInducedSubgraphFactory());
    }
}