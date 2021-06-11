package com.fuljo.polimi.middleware.mask_the_week;

/**
 * Represents a specific schema of a CSV file
 */
public enum CSVFormat {
    /** Reports from the European Center for Disease control */
    ECDC,
    /** Reports produced by our virus spreading simulator https://github.com/fuljo/my-population-infection */
    MPI
}
