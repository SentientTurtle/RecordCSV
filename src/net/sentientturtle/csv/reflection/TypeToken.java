package net.sentientturtle.csv.reflection;

import net.sentientturtle.csv.ThrowingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * Token that represents generic types.
 * <br>
 * Instantiated by (anonymous) subclass. (Example: {@code new TypeToken<List<String>>() {};})
 * <br>
 * For non-parameterized types, the {@link TypeToken#TypeToken(Class)} constructor is available (Example: {@code new TypeToken<>(String.class)})
 * This constructor may not be used for parameterized types
 * <br>
 * <br>
 * Tokens must specify a concrete type, and may not include type variables (E.g. {@code TypeToken<List<T>>, TypeToken<List<T[]>> are invalid}.
 * An exception is thrown by the constructor if this requirement is not met.
 * <br>
 * Caution: Type inference should be avoided when creating tokens, as no error is thrown when no types are inferred. A token representing {@link Object} may accidentally created
 *
 * @param <T> Type which this token represents
 */
public class TypeToken<T> {
    /*
     * TypeToken wraps around {@link TypeToken.Token}; This makes instantiation for subclasses easier.
     */
    private final Token inner;

    /**
     * Unvalidated constructor, should only be used in conjunction with {@link TypeToken#createToken(Type, Token.Concrete, boolean)} or from components of Token instances (superclasses, type parameters, etc)
     */
    private TypeToken(Token inner) {
        this.inner = inner;
    }

    /**
     * Instantiates and validates a Token from a given Type
     * <br>Type must be concrete, either having no type variables, or having only type variables from <i>parent</i>
     * @param type Type to represent
     * @param parent Parent type to fill in type parameters from
     * @param permitWildcard If true, permit this type to represent a wildcard
     * @return Token representing type
     * @throws IllegalArgumentException if an unrepresentable type was specified
     */
    private static Token createToken(Type type, @Nullable Token.Concrete parent, boolean permitWildcard) throws IllegalArgumentException {
        if (type instanceof ParameterizedType parameterizedType) {
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            Token[] typeParameters = new Token[actualTypeArguments.length];
            for (int i = 0; i < actualTypeArguments.length; i++) {
                typeParameters[i] = createToken(actualTypeArguments[i], parent, true);   // Parameters are checked to be concrete by recursion
            }
            return new Token.Concrete(rawType, typeParameters);
        } else if (type instanceof Class<?> clazz) { // Non-generic type
            if (clazz.getTypeParameters().length == 0) {
                return new Token.Concrete(clazz, new Token[0]);
            } else {
                throw new IllegalArgumentException("Raw use of type " + clazz + "; This type must be parameterized");
            }
        } else if (type instanceof GenericArrayType arrayType) {
            return createToken(arrayType.getGenericComponentType(), parent, permitWildcard)
                           .asArray();
        } else if (type instanceof TypeVariable<?> typeVariable) {
            if (parent != null) {
                if (parent.rawType.equals(typeVariable.getGenericDeclaration())) {
                    TypeVariable<? extends Class<?>>[] rawTypeTypeParameters = parent.rawType.getTypeParameters();
                    for (int j = 0; j < rawTypeTypeParameters.length; j++) {
                        if (rawTypeTypeParameters[j].equals(typeVariable)) {
                            return parent.typeParameters[j];
                        }
                    }
                    throw new IllegalArgumentException("could not find type variable " + typeVariable + " in declaring class " + parent);
                } else {
                    throw new IllegalArgumentException("encountered type variable declared by " + typeVariable.getGenericDeclaration() + " instead of " + parent);
                }
            } else {
                throw new IllegalArgumentException("type described by this token must be a concrete/non-generic type; A generic type (" + typeVariable.getName() + ") was specified instead");
            }
        } else if (type instanceof WildcardType wildcard) {
            if (permitWildcard) {
                Type[] upperBounds = wildcard.getUpperBounds();
                Type[] lowerBounds = wildcard.getLowerBounds();

                Token[] upper = null;
                if (upperBounds != null && upperBounds.length > 0 && !(upperBounds.length == 1 && Object.class.equals(upperBounds[0]))) {   // upper/lowerBounds are [Object.class] by default. We disregard this as all generic values must be Objects
                    upper = new Token.Concrete[upperBounds.length];
                    for (int i = 0; i < upperBounds.length; i++) {
                        upper[i] = createToken(upperBounds[i], parent, false);
                    }
                }
                Token[] lower = null;
                if (lowerBounds != null && lowerBounds.length > 0 && !(lowerBounds.length == 1 && Object.class.equals(lowerBounds[0]))) {
                    lower = new Token.Concrete[lowerBounds.length];
                    for (int i = 0; i < lowerBounds.length; i++) {
                        lower[i] = createToken(lowerBounds[i], parent, false);
                    }
                }
                return new Token.Bound(upper, lower, 0);
            } else {
                // Generally, wildcard types like `TypeToken<?>(){}` cannot be instantiated, leaving this error unreachable
                throw new IllegalArgumentException("type described by this token must be a concrete/non-generic type; A top level wildcard (" + wildcard + ") was specified instead");
            }
        } else {
            // At time of writing, no other Type implementers exist. Should any be added in future Java versions and encountered, an error is thrown as we cannot guarantee recordType is a concrete and instantiable type.
            throw new IllegalArgumentException("described type cannot be validated, type of kind `" + type.getClass() + "` is not supported by TypeToken implementation. This is a bug, please open an issue.");  // TODO: Specify name of our library once chosen.
        }
    }

    /**
     * Convenience constructor for un-parameterized types (such as `String`)
     * <br> This constructor does not accept parameterized types (such as {@link java.util.List `List`})
     * @param aClass Class this token represents
     * @throws IllegalArgumentException if this constructor was called with a parameterized type
     */
    public TypeToken(Class<T> aClass) throws IllegalArgumentException {
        if (aClass.getTypeParameters().length > 0) throw new IllegalArgumentException("This constructor may only be used with classes that have no type parameters, please create an anonymous subclass instead.");
        if (!this.getClass().equals(TypeToken.class)) throw new IllegalArgumentException("Anonymous subclassing of TypeToken must use the no-args constructor, the represented type is captured through generic parameter T");
        this.inner = createToken(aClass, null, false);
    }

    /**
     * Main constructor for TypeToken, invoked by creating an anonymous subclass
     * @throws IllegalArgumentException if constructing a non-concrete typetoken variant. See class docs.
     */
    protected TypeToken() throws IllegalArgumentException {
        // Ensure any user-created tokens only directly extend TypeToken;
        if (this.getClass().getSuperclass() != TypeToken.class) throw new IllegalArgumentException("TypeToken must be directly subclassed for each token!");

        var superType = (ParameterizedType) this.getClass().getGenericSuperclass();    // This cast is safe as the above guard ensures only TypeToken may be the superclass, and TypeToken is a parameterized type.
        Type type = superType.getActualTypeArguments()[0];    // TypeToken has exactly 1 generic parameter, so this array-index is safe

        this.inner = createToken(type, null, false);    // TODO: Maybe permit wildcards
    }


    /**
     * @return True if this token represents a concrete type
     */
    public final boolean isConcreteType() {
        return this.inner instanceof Token.Concrete;
    }

    /**
     * @return True if this token represents a generic-bound type
     */
    public final boolean isGenericBound() {
        return this.inner instanceof Token.Bound;
    }

    /**
     * Equivalent to {@link Class#isRecord()}
     * @return True if the represented type is a record
     */
    public final boolean isRecord() {
        if (this.inner instanceof Token.Concrete concrete) {
            return concrete.rawType.isRecord();
        } else {
            return false;
        }
    }

    /**
     * @return Class of the type represented by this type token, or null if this is a generic-bound token
     */
    @SuppressWarnings("unchecked")
    public final @Nullable Class<T> getRawType() {
        if (this.inner instanceof Token.Concrete concreteToken) {
            return (Class<T>) concreteToken.rawType;
        } else {
            return null;
        }
    }

    /**
     * @return Generic parameters of the type represented by this token, a 0-length array if this type has no generic parameters, or null if this is a generic-bound token
     */
    public final TypeToken<?> @Nullable [] getGenericParameters() {
        if (this.inner instanceof Token.Concrete concreteToken) {
            TypeToken<?>[] typeTokens = new TypeToken<?>[concreteToken.typeParameters.length];
            for (int i = 0; i < concreteToken.typeParameters.length; i++) {
                typeTokens[i] = new TypeToken<>(concreteToken.typeParameters[i]);
            }
            return typeTokens;
        } else {
            return null;
        }
    }

    /**
     * @return Supertype of this type, returning null if this type represents {@link Object} or is a generic-bound type
     */
    public final @Nullable TypeToken<?> getSupertype() throws UnsupportedOperationException {
        if (this.inner instanceof Token.Concrete concreteToken) {
            Type supertype = concreteToken.rawType.getGenericSuperclass();
            if (supertype instanceof Class<?> superclass) {
                return new TypeToken<>(new Token.Concrete(superclass, new Token[0]));
            } else if (supertype instanceof ParameterizedType parameterizedSupertype) {
                return new TypeToken<>(createToken(parameterizedSupertype, concreteToken, false));
            } else if (supertype == null) {
                return null;
            } else {
                // Unreachable under current reflection API, throw an exception should API change.
                throw new IllegalStateException("supertype has unsupported type `" + supertype.getClass() + "`; This is a bug!");   // TODO: library name
            }
        } else {
            return null;
        }
    }

    /**
     * @return Token representing an array type whose component type is this TypeToken
     */
    public final TypeToken<T[]> asArray() {
        return new TypeToken<>(this.inner.asArray());
    }

    /**
     * Token representing a Record field
     * @param name Name of field
     * @param type Type of field
     */
    public record RecordField(String name, TypeToken<?> type) {}

    /**
     * @return Record components if this token represents a record
     * @throws IllegalStateException if this token does not represent a record
     */
    public final RecordField[] getRecordComponents() throws IllegalStateException {
        if (this.inner instanceof Token.Concrete concreteToken) {
            RecordComponent[] recordComponents = concreteToken.rawType.getRecordComponents();
            if (recordComponents == null) throw new IllegalStateException("token does not represent a record");

            RecordField[] recordFields = new RecordField[recordComponents.length];
            for (int i = 0; i < recordComponents.length; i++) {
                recordFields[i] = new RecordField(recordComponents[i].getName(), new TypeToken<>(createToken(recordComponents[i].getGenericType(), concreteToken, true)));
            }
            return recordFields;
        } else {
            throw new IllegalStateException("token does not represent a record");
        }
    }

    /**
     * @return record canonical constructor for the record represented by this token
     * @throws NoSuchMethodException if constructor cannot be resolved
     * @throws IllegalStateException if this token does not represent a record
     */
    public final ThrowingFunction<Object[], T> getRecordConstructor() throws NoSuchMethodException, IllegalStateException {
        if (this.inner instanceof Token.Concrete concreteToken) {
            @SuppressWarnings("unchecked")
            Class<T> rawType = (Class<T>) concreteToken.rawType;
            RecordComponent[] recordComponents = rawType.getRecordComponents();
            if (recordComponents == null) throw new IllegalStateException("token does not represent a record");

            Class<?>[] parameters = new Class<?>[recordComponents.length];
            for (int i = 0; i < recordComponents.length; i++) {
                parameters[i] = recordComponents[i].getType();
            }
            try {
                Constructor<T> constructor = rawType.getConstructor(parameters);
                return constructor::newInstance;
            } catch (NoSuchMethodException e) {
                int modifiers = rawType.getModifiers();
                if (!Modifier.isPublic(modifiers)) throw new NoSuchMethodException("cannot find canonical constructor for record; Ensure that the record is public");
                // TODO: Insert error message when record isn't accessible b/c of java 9 packages
                throw new NoSuchMethodException("cannot find canonical constructor for record: " + e.getMessage());
            }
        } else {
            throw new IllegalStateException("token does not represent a record");
        }
    }

    @Override
    public final String toString() {
        return inner.toString();
    }

    /**
     * Equality override
     * <br>
     * NOTE: Types are only equal if they have the same raw types, and their generic parameters match exactly (e.g. {@code TypeToken<List<?>>} != {@code TypeToken<List<Object>>})
     */
    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeToken<?> typeToken)) return false;   // We must accept subclasses, as our mechanism for capturing generic types relies on (anonymous) subclasses to work
        return inner.equals(typeToken.inner);
    }

    /**
     * Hashcode override
     * <br>
     * NOTE: Types are only equal if they have the same raw types, and their generic parameters match exactly (e.g. {@code TypeToken<List<?>>} != {@code TypeToken<List<Object>>})
     */
    @Override
    public final int hashCode() {
        return inner.hashCode(); // Delegate to token subtype; Actual implementation auto-generated as token subtypes are records
    }

    /**
     * Token implementation
     */
    private sealed interface Token {

        /**
         * @return Token representing an array of this token ({@code T -> T[]})
         */
        Token asArray();

        /**
         * Token type representing concrete types
         * @param rawType Class of the represented type
         * @param typeParameters Generic parameters of the represented type. Must not be null. Types without generic parameters are represented with a 0-length {@code typeParameters} array.
         */
        record Concrete(Class<?> rawType, Token @NotNull [] typeParameters) implements Token {

            /**
             * @return Token representing an array of this token ({@code T -> T[]})
             */
            @Override
            public Concrete asArray() {
                return new Token.Concrete(this.rawType.arrayType(), this.typeParameters);
            }

            @Override
            public String toString() {
                int arrayCount = 0;
                Class<?> componentType = this.rawType;
                while (componentType.getComponentType() != null) {
                    arrayCount += 1;
                    componentType = componentType.getComponentType();
                }
                if (this.typeParameters.length > 0) {
                    String[] parameters = new String[this.typeParameters.length];
                    for (int i = 0; i < this.typeParameters.length; i++) {
                        parameters[i] = this.typeParameters[i].toString();
                    }
                    return componentType.getName() + "<" + String.join(", ", parameters) + ">" + ("[]".repeat(arrayCount));
                } else {
                    return componentType.getName() + ("[]".repeat(arrayCount));
                }
            }

            // Equals must be overridden for records with an array field
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof Concrete concrete)) return false;
                return rawType.equals(concrete.rawType) && Arrays.equals(typeParameters, concrete.typeParameters);
            }

            @Override
            public int hashCode() {
                int result = Objects.hash(rawType);
                result = 31 * result + Arrays.hashCode(typeParameters);
                return result;
            }
        }

        /**
         * Token type representing generic bounds ({@code ? extends T}, or an array whose component type is a generic bound.
         * @param upperBounds Tokens representing the upper bounds ({@code ? extends T & U & V}
         * @param lowerBounds Tokens representing the lower bounds ({@code ? super T & U & V}
         * @param arrayCount The dimensionality of the array represented by this token, or 0 if representing a generic bound
         */
        record Bound(Token @Nullable [] upperBounds, Token @Nullable [] lowerBounds, int arrayCount) implements Token {

            /**
             * @return Token representing an array of this token ({@code T -> T[]})
             */
            @Override
            public Bound asArray() {
                return new Token.Bound(this.upperBounds, this.lowerBounds, this.arrayCount + 1);
            }

            @Override
            public String toString() {
                String[] upper = null;
                String[] lower = null;
                if (upperBounds != null) {
                    upper = new String[this.upperBounds.length];
                    for (int i = 0; i < this.upperBounds.length; i++) {
                        upper[i] = this.upperBounds[i].toString();
                    }
                }
                if (lowerBounds != null) {
                    lower = new String[lowerBounds.length];
                    for (int i = 0; i < lowerBounds.length; i++) {
                        lower[i] = lowerBounds[i].toString();
                    }
                }
                if (upper == null && lower == null) {
                    return "?" + ("[]".repeat(arrayCount));
                } else if (upper != null && lower == null) {
                    return "?" + ("[]".repeat(arrayCount)) + " extends " + String.join(" & ", upper);
                } else if (upper == null && lower != null) {
                    return "?" + ("[]".repeat(arrayCount)) + " super " + String.join(" & ", lower);
                } else {
                    return "?" + ("[]".repeat(arrayCount)) + " extends " + String.join(" & ", upper) + "; super " + String.join(" & ", lower);
                }
            }

            // Equals must be overridden for records with an array field
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof Bound bound)) return false;
                return arrayCount == bound.arrayCount && Arrays.equals(upperBounds, bound.upperBounds) && Arrays.equals(lowerBounds, bound.lowerBounds);
            }

            @Override
            public int hashCode() {
                int result = Objects.hash(arrayCount);
                result = 31 * result + Arrays.hashCode(upperBounds);
                result = 31 * result + Arrays.hashCode(lowerBounds);
                return result;
            }
        }
    }
}
