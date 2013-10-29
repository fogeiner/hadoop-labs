package pagerank;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PageRank {

    private static class Triple {

        private Integer row;
        private Integer column;
        private Double value;

        public Triple(Integer row, Integer column, Double value) {
            this.row = row;
            this.column = column;
            this.value = value;
        }

        public Integer getRow() {
            return row;
        }

        public void setRow(Integer row) {
            this.row = row;
        }

        public Integer getColumn() {
            return column;
        }

        public void setColumn(Integer column) {
            this.column = column;
        }

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Triple{" + "row=" + row + ", column=" + column + ", value=" + value + '}';
        }

    }

    private static List<Double> multiplyTransposedMatrixVector(List<Triple> matrix, List<Double> vector) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < vector.size(); ++i) {
            result.add(.0);
        }

        for (Triple t : matrix) {
            int i = t.getRow() - 1;
            int j = t.getColumn() - 1;
            result.set(j, result.get(j) + t.getValue() * vector.get(i));
        }

        return result;
    }

    private static double distance(List<Double> v1, List<Double> v2) {
        if (v1.size() != v2.size()) {
            throw new IllegalArgumentException("Vectors cannot have different lengths");
        }
        double diff = 0;
        for (int i = 0; i < v1.size(); ++i) {
            diff += Math.abs(v1.get(i) - v2.get(i));
        }
        return diff;
    }

    public static void main(String[] args) {
        int MATRIX_SIZE = 3;
        double beta = 0.8;

        List<Map.Entry<Integer, Integer>> edges = new ArrayList<>();
        {
            edges.add(new AbstractMap.SimpleEntry<>(2, 1));
            edges.add(new AbstractMap.SimpleEntry<>(1, 2));
            edges.add(new AbstractMap.SimpleEntry<>(1, 1));
            edges.add(new AbstractMap.SimpleEntry<>(2, 3));
            edges.add(new AbstractMap.SimpleEntry<>(3, 3));
        }

        List<Triple> matrix = new ArrayList<>();
        for (int row = 1; row <= MATRIX_SIZE; ++row) {
            int count = 0;
            for (Map.Entry<Integer, Integer> edge : edges) {
                if (edge.getKey() == row) {
                    count++;
                }
            }
            for (int colomn = 1; colomn <= MATRIX_SIZE; ++colomn) {
                double value;
                if (count != 0) {
                    value = (1 - beta) * 1 / MATRIX_SIZE;
                    for (Map.Entry<Integer, Integer> edge : edges) {
                        if (edge.getKey() == row && edge.getValue() == colomn) {
                            value += 1.0 / count * beta;
                            break;
                        }
                    }
                } else {
                    value = 1.0 / MATRIX_SIZE;
                }
                matrix.add(new Triple(row, colomn, value));
            }
        }

        for (Triple t : matrix) {
            System.out.println(t.toString());
        }

        List<Double> vector = new ArrayList<>();

        for (int i = 0; i < MATRIX_SIZE; ++i) {
            vector.add(1.0 / MATRIX_SIZE);
        }
        double EPS = 1e-5;
        double distance;

        int steps = 0;
        do {
            List<Double> newVector = multiplyTransposedMatrixVector(matrix, vector);
            distance = distance(vector, newVector);
            steps++;

            Collections.copy(vector, newVector);

            System.out.println(vector);
        } while (distance > EPS);

        System.out.println("Iterations: " + steps + "\nResult: " + vector);
    }
}
