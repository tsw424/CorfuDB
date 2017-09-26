package org.corfudb.runtime.object;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.UUID;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.serializer.ISerializer;

/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {

    static <T> VersionedObjectManager<T> generateManager(ICorfuSMR<T> w, ObjectBuilder<T> builder) {
        return new VersionedObjectManager<>(
                () -> {
                    try {
                        T ret = null;
                        if (builder.getArguments() == null ||
                                builder.getArguments().length == 0) {
                            ret = builder.getType().newInstance();
                        } else {
                            // This loop is not ideal, but the easiest way to get around Java boxing,
                            // which results in primitive constructors not matching.
                            for (Constructor<?> constructor : builder
                                    .getType().getDeclaredConstructors()) {
                                try {
                                    ret = (T) constructor.newInstance(builder.getArguments());
                                    break;
                                } catch (Exception e) {
                                    // just keep trying until one works.
                                }
                            }
                        }
                        if (ret instanceof ICorfuSMRProxyWrapper) {
                            ((ICorfuSMRProxyWrapper<T>) ret).setProxy$CORFU(null);
                        }
                        return ret;
                    } catch (InstantiationException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
                ,
                new StreamViewSMRAdapter(
                        builder.getRuntime(),
                        builder.getRuntime()
                                .getStreamsView().get(builder.getStreamId())),
                w, builder);
    }

    /**
     * Returns a wrapper for the underlying SMR Object
     *
     * @param <T>        Type
     * @return Returns the wrapper to the object.
     * @throws ClassNotFoundException Class T not found.
     * @throws IllegalAccessException Illegal Access to the Object.
     * @throws InstantiationException Cannot instantiate the object using the arguments and class.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    public static <T> T getWrapper(ObjectBuilder<T> builder)
            throws ClassNotFoundException, IllegalAccessException,
            InstantiationException {
        // Do we have a compiled wrapper for this type?
        Class<ICorfuSMR<T>> wrapperClass = (Class<ICorfuSMR<T>>)
                Class.forName(builder.getType().getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuSMR<T> wrapperObject = null;
        if ((builder.getArguments() == null || builder.getArguments().length == 0)) {
            try {
                wrapperObject = wrapperClass.getDeclaredConstructor(IManagerGenerator.class)
                        .newInstance((IManagerGenerator<T>) w -> generateManager(w, builder));
            } catch (Exception e) {
                throw new RuntimeException("Failed to build object", e);
            }
        } else {
            // This loop is not ideal, but the easiest way to get around Java
            // boxing, which results in primitive constructors not matching.
            for (Constructor<?> constructor : wrapperClass
                    .getDeclaredConstructors()) {
                try {
                    Object[] arguments = new Object[builder.getArguments().length + 1];
                    IManagerGenerator<T> generator = w -> generateManager(w, builder);
                    arguments[0] = generator;
                    for (int i = 0; i < builder.getArguments().length; i++) {
                        arguments[i + 1] = builder.getArguments()[i];
                    }
                    wrapperObject = (ICorfuSMR<T>) constructor.newInstance(arguments);
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    // just keep trying until one works.
                }
            }
        }

        if (wrapperObject == null) {
            throw new RuntimeException("Failed to generate object, all target constructors" +
                    " exhausted");
        }

        return (T) wrapperObject;
    }
}
